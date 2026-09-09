/**
 * The two non-HTTP invocation paths: a schedule firing, and the reconciler's own tick.
 *
 * Both exist because DynamoDB is the source of truth and EventBridge Scheduler is only the timer.
 * Every disagreement between the two resolves towards the DynamoDB row.
 */

import { randomUUID } from 'node:crypto';

import { DateTime } from 'luxon';

import {
  buildV1Message,
  calculateNextTriggerDate,
  commitChainStep,
  commitChainStepLegacy,
  COMMAND,
  createSchedule,
  deleteSchedule,
  env,
  getConfig,
  getRow,
  isV2Row,
  listSchedules,
  publish,
  queryByPolicy,
  queryByStatus,
  scheduleNameFor,
  shouldCreateNext,
  STATUS,
  transition,
} from './core.mjs';

/** Only names this PoC minted are candidates for orphan deletion; Terraform owns the reconciler's. */
const OWNED_NAME =
  /^poc-schedule-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

// ---------------------------------------------------------------------------------------------
// Dispatcher
// ---------------------------------------------------------------------------------------------

export async function dispatch(scheduleId, log) {
  const row = await getRow(scheduleId);

  // The timer fired for something the store has no row for. The row is the truth, so the timer is
  // wrong: nothing is published, and the reconciler deletes the schedule on its next tick.
  if (!row) {
    log('dispatch', 'orphanFire', { scheduleId });
    await deleteSchedule(scheduleId);
    return { fired: false, reason: 'noRow' };
  }

  /**
   * The conditional claim, accepting a row that is already `processing`.
   *
   * Lambda's own async retries of a failed fire are the *same* fire, so refusing a `processing`
   * row would make attempt two return success and the failure destination would never receive
   * anything — the failure would vanish, which is the behaviour this whole exercise exists to
   * remove. A row that reached `executed` or `cancelled` is still refused, so a duplicate delivery
   * of a fire that already succeeded publishes nothing.
   *
   * The exposure this leaves is real and belongs in the design, not hidden here: a fire that
   * published and then failed will publish again on retry. At-least-once, which is what the old
   * service also gives, and which the consumer has to be idempotent about.
   */
  const claimed = await transition(
    row.id,
    [STATUS.Pending, STATUS.Processing],
    STATUS.Processing
  );
  if (!claimed) {
    log('dispatch', 'notClaimable', { scheduleId, status: row.status });
    return { fired: false, reason: 'notClaimable', status: row.status };
  }

  const config = await getConfig();

  /**
   * V1 has its envelope minted for it; a V2 row carries the whole `ISendMessageRequest` the caller
   * handed over and the old service passes it to `sendMessageV2` untouched, topic included. So the
   * topic for a V2 fire comes off the message, not out of the context — which also means a V2 row
   * naming a `.fifo` topic gets the same ordering guarantee a V1 row does.
   */
  const message = isV2Row(claimed)
    ? {
        topicName: claimed.message?.topicName,
        payload: claimed.message?.payload ?? claimed.message,
        messageGroupId: claimed.message?.messageGroupId ?? claimed.policyId,
      }
    : buildV1Message(claimed);

  try {
    if (config.breakTarget) {
      throw new Error('simulated publish failure (POC breakTarget)');
    }

    await publish(message);

    const next = await buildNextOccurrence(claimed, log);

    if (config.legacyChain) {
      const result = await commitChainStepLegacy(claimed, next, {
        breakNext: Boolean(config.breakNext),
      });
      if (result.swallowed) {
        log('dispatch', 'chainBrokenSilently', {
          scheduleId,
          detail: result.swallowed,
        });
        return { fired: true, chainBroken: true, next: null };
      }
    } else {
      if (config.breakNext) {
        throw new Error('simulated chain-write failure (POC breakNext)');
      }
      await commitChainStep(claimed, next);
    }

    // The timer for the next occurrence. If this throws the row is already `pending`, so the
    // reconciler creates the missing schedule — the chain cannot be lost by a failure here.
    if (next) await createSchedule(next);

    log('dispatch', 'fired', {
      scheduleId,
      nextId: next?.id ?? null,
      nextTriggerAt: next?.triggerAt ?? null,
    });

    return { fired: true, chainBroken: false, next: next?.id ?? null };
  } catch (error) {
    /**
     * The throw always propagates, because that is what makes Lambda retry and, on the last
     * attempt, write the failure to the DLQ through its event-invoke destination. Scheduler's own
     * dead-letter queue would never see any of this: the invoke is asynchronous, so a function that
     * runs and throws is a *delivered* fire as far as Scheduler is concerned.
     *
     * What changes is where the row is left. Lambda does not tell a function which retry it is on,
     * so the row counts for itself: on the final attempt it is parked at `failed` before the throw,
     * which means the status flips in the same moment the dead-letter record appears rather than a
     * reconciler tick later. Every earlier attempt leaves it `processing` to be retried.
     */
    const attempts = (claimed.attempts ?? 0) + 1;
    const exhausted = attempts >= env.invocationsPerFiring;

    await transition(
      row.id,
      STATUS.Processing,
      exhausted ? STATUS.Failed : STATUS.Processing,
      {
        attempts,
        lastError: String(error.message).slice(0, 500),
        ...(exhausted ? { failedAt: new Date().toISOString() } : {}),
      }
    );

    log('dispatch', exhausted ? 'fireGaveUp' : 'fireFailed', {
      scheduleId,
      attempt: attempts,
      of: env.invocationsPerFiring,
      reason: error.message,
    });

    throw error;
  }
}

/**
 * The next link in a recurrence chain, or null when the chain ends here.
 *
 * Ported from `executeActionV1`/`executeActionV2`, including the `processObligation` branch that
 * only V1 rows take.
 */
async function buildNextOccurrence(row, log) {
  if (row.period === null || row.period === undefined) return null;

  const nextTriggerAt = calculateNextTriggerDate(row);

  if (!shouldCreateNext(row.endAt, nextTriggerAt)) {
    log('dispatch', 'chainEnded', { scheduleId: row.id, reason: 'endAt' });
    return null;
  }

  if (!isV2Row(row) && row.context?.command === COMMAND.ProcessObligation) {
    const siblings = await queryByPolicy(row.policyId);
    const conclude = siblings.find(
      (item) => item.context?.name === COMMAND.ConcludePolicy
    );

    if (!conclude) {
      // The old service indexes `[0]` on this empty result and throws a TypeError, which its catch
      // block swallows — leaving the row `executed` and the chain dead. Reported rather than
      // reproduced: a PoC that copied the crash would prove nothing.
      log('dispatch', 'processObligationWithoutConclude', {
        scheduleId: row.id,
        policyId: row.policyId,
      });
    } else if (
      DateTime.fromISO(nextTriggerAt) > DateTime.fromISO(String(conclude.triggerAt))
    ) {
      log('dispatch', 'chainEnded', {
        scheduleId: row.id,
        reason: 'afterConcludePolicy',
      });
      return null;
    }
  }

  const processingData =
    row.processingData !== undefined && row.processingData !== null
      ? { ...row.processingData, counter: row.processingData.counter + 1 }
      : undefined;

  const now = new Date().toISOString();

  // Spreading the fired row carries every field forward, so anything the dispatcher writes onto a
  // row during a failed attempt has to be reset here explicitly or it pollutes the whole chain.
  // Today that is `attempts` and `lastError`; adding a third field means adding it below too.
  return {
    ...row,
    id: randomUUID(),
    status: STATUS.Pending,
    triggerAt: nextTriggerAt,
    processingData,
    attempts: 0,
    deliveries: 1,
    lastError: undefined,
    failedAt: undefined,
    createdAt: now,
    updatedAt: now,
  };
}

// ---------------------------------------------------------------------------------------------
// Reconciler
// ---------------------------------------------------------------------------------------------

/**
 * Every 2 minutes in the PoC, every 15 in the design. Three repairs, all resolving towards the row:
 *
 *  - a `pending` row with no timer gets one, so a lost CreateSchedule cannot lose a fire;
 *  - a timer with no `pending` row is deleted, so a cancelled schedule cannot fire late;
 *  - a row stuck `processing` past the TTL goes back to `pending`, which is what the old service's
 *    `resetLockedScheduledActions` job did and the reason that job exists at all.
 */
export async function reconcile(log) {
  const [pending, processing, names, config] = await Promise.all([
    queryByStatus(STATUS.Pending),
    queryByStatus(STATUS.Processing),
    listSchedules(),
    getConfig(),
  ]);

  // Read from the config row, not the environment, so the console can change it mid-demo.
  const maxDeliveries = config.maxDeliveryAttempts ?? env.maxDeliveryAttempts;

  const live = new Set(names);
  const wanted = new Set(pending.map((row) => scheduleNameFor(row.id)));

  let repaired = 0;
  for (const row of pending) {
    if (live.has(scheduleNameFor(row.id))) continue;
    await createSchedule(row);
    repaired += 1;
    log('reconciler', 'timerRepaired', {
      scheduleId: row.id,
      triggerAt: row.triggerAt,
    });
  }

  let orphansDeleted = 0;
  for (const name of names) {
    if (!OWNED_NAME.test(name) || wanted.has(name)) continue;
    await deleteScheduleByName(name);
    orphansDeleted += 1;
    log('reconciler', 'orphanDeleted', { name });
  }

  /**
   * A row stuck in `processing` past the TTL has had its firing exhausted — Lambda retried it twice
   * and wrote a dead-letter record. The question is whether to fire it again.
   *
   * The old service always did, with no cap, which is an unbounded loop against a permanently
   * broken target: one dead-letter record per cycle, for ever. Here the row gets
   * `max_delivery_attempts` firings and then stops at `failed`, which is terminal — nothing
   * re-fires it and no timer is created. Recovering one is a deliberate act, not a side effect.
   */
  const cutoff = DateTime.utc().minus({ minutes: env.processingTtlMinutes });
  let unstuck = 0;
  let failed = 0;

  for (const row of processing) {
    if (DateTime.fromISO(String(row.updatedAt)) > cutoff) continue;

    const used = row.deliveries ?? 1;

    if (used >= maxDeliveries) {
      const done = await transition(row.id, STATUS.Processing, STATUS.Failed, {
        failedAt: new Date().toISOString(),
      });
      if (!done) continue;
      failed += 1;
      log('reconciler', 'gaveUp', {
        scheduleId: row.id,
        deliveries: used,
        attempts: row.attempts ?? 0,
        lastError: row.lastError ?? null,
      });
      continue;
    }

    const back = await transition(row.id, STATUS.Processing, STATUS.Pending, {
      deliveries: used + 1,
    });
    if (!back) continue;
    await createSchedule(back);
    unstuck += 1;
    log('reconciler', 'unstuck', {
      scheduleId: row.id,
      delivery: used + 1,
      of: maxDeliveries,
      attempts: row.attempts ?? 0,
      lastError: row.lastError ?? null,
    });
  }

  const result = {
    repaired,
    orphansDeleted,
    unstuck,
    failed,
    pending: pending.length,
    processing: processing.length,
  };

  // Logged on every tick, including the quiet ones. A reconciler that only speaks when it repairs
  // something is indistinguishable from a reconciler that is not running at all, and "is it even
  // running" is the first thing anyone watching a repair demo wants to know.
  log('reconciler', 'tick', result);

  return result;
}

async function deleteScheduleByName(name) {
  const id = name.replace(/^poc-schedule-/, '');
  await deleteSchedule(id);
}
