/**
 * The schedule-platform PoC Lambda. One artifact, four ways in — the same split the scaffold in
 * services-monorepo#2561 established, and the guards below are that PR's guards.
 *
 * The seven management routes are the complete inbound surface of `services/schedule`, ported
 * route for route so a caller cutover needs no client change beyond the transport.
 */

import {
  CONFIG_ID,
  createSchedule,
  deleteRow,
  deleteSchedule,
  drainQueue,
  env,
  FEED_DLQ,
  FEED_FIRED,
  forceStatus,
  getConfig,
  getRow,
  newSimpleRow,
  listSchedules,
  newRow,
  putRow,
  queryByPolicy,
  queueDepth,
  readLogs,
  scanRows,
  retimePatch,
  setConfig,
  splitStatuses,
  updateSchedule,
  STATUS,
  transition,
  validateActivate,
  validateCancel,
  validateCancelPolicy,
  validateCreateSimple,
  validateCreateV1,
  validateCreateV2,
  validateGet,
  validateIsoDate,
} from './core.mjs';
import { dispatch, reconcile } from './dispatch.mjs';

// ---------------------------------------------------------------------------------------------
// Invocation guards — from serverless/schedule/src/invocation.guards.ts
// ---------------------------------------------------------------------------------------------

const isObject = (value) =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/** A management call, hand-built by the caller as the payload of one `lambda:Invoke`. */
const isHttpInvoke = (event) =>
  isObject(event) &&
  isObject(event.requestContext) &&
  isObject(event.requestContext.http);

const isDispatch = (event) =>
  isObject(event) &&
  typeof event.scheduleId === 'string' &&
  event.scheduleId.trim() !== '';

/**
 * `source` is `"reconciler"` and NOT `"aws.events"`. Scheduler passes a target's `Input` through
 * untouched, unlike an EventBridge *rule*, so the tick arrives as the literal value Terraform sets.
 */
const isReconciler = (event) =>
  isObject(event) && event.source === 'reconciler';

// ---------------------------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------------------------

export async function handler(event) {
  const log = (component, message, detail = {}) => {
    // The shape is logged and never a whole payload: a schedule context can hold customer data.
    //
    // `detail` is spread first so it can never shadow `component` or `message`. It did once: a
    // detail carrying its own `message` key silently replaced the label, and the line still looked
    // plausible enough that the loss went unnoticed.
    console.log(JSON.stringify({ ...detail, component, message }));
  };

  if (isHttpInvoke(event)) return route(event, log);
  if (isDispatch(event)) return dispatch(event.scheduleId, log);
  if (isReconciler(event)) return reconcile(log);

  log('handler', 'unknownInvocationPayload', {
    keys: isObject(event) ? Object.keys(event) : [],
  });
  return { ignored: true };
}

// ---------------------------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------------------------

const json = (statusCode, body) => ({
  statusCode,
  headers: { 'content-type': 'application/json' },
  body: JSON.stringify(body),
});

const badRequest = (errors) => json(400, { message: 'validation failed', errors });

async function route(event, log) {
  const { method, path } = event.requestContext.http;
  const query = event.queryStringParameters ?? {};
  let body;

  try {
    body = event.body ? JSON.parse(event.body) : undefined;
  } catch {
    return badRequest(['body is not valid JSON']);
  }

  const handlerFn = ROUTES[`${method} ${path}`];
  if (!handlerFn) {
    log('api', 'unknownRoute', { method, path });
    return json(404, { error: 'unknown route' });
  }

  try {
    return await handlerFn({ body, query, log });
  } catch (error) {
    log('api', 'routeFailed', { method, path, reason: error.message });
    return json(500, { error: error.message });
  }
}

// ---------------------------------------------------------------------------------------------
// The seven management routes
// ---------------------------------------------------------------------------------------------

const ROUTES = {
  /**
   * The dumb-service surface. Three routes, no domain: create one schedule, cancel some by id,
   * release some by id. The `/v1` and `/v2` routes below are the ported inbound surface of the
   * service being replaced and are left alone; the console simply stopped using them.
   */
  'POST /schedule': async ({ body, log }) => {
    const errors = validateCreateSimple(body);
    if (errors.length) return badRequest(errors);

    const row = newSimpleRow(body);
    await putRow(row);
    await createSchedule(row);

    log('api', 'created', {
      scheduleId: row.id,
      triggerAt: row.triggerAt,
      period: row.period,
    });

    return json(201, {
      created: [{ id: row.id, status: row.status, triggerAt: row.triggerAt }],
    });
  },

  /**
   * Retime one schedule.
   *
   * The design has this as `PATCH /v1/schedules/:id` — "retime or repayload"; the id is in the body
   * here only because this router matches whole paths and a demo does not need path parameters.
   *
   * The service being replaced has no update at all, which is why `workflow` moves a termination
   * date by cancelling and creating — "two schedules' worth of failure modes for one intention",
   * as the design puts it. Nothing forbade an update; there was simply never one to call.
   */
  'POST /schedule/update': async ({ body, log }) => {
    if (!validateIsoDate(body?.triggerAt)) {
      return badRequest(['triggerAt must be an ISO date']);
    }

    const row = await getRow(body?.scheduleId);
    if (!row) return json(404, { error: 'no such schedule' });

    /**
     * Only a row that has not fired yet. `executed` is history, `cancelled` and `failed` are
     * terminal — retiming any of them would be inventing a resurrection path nobody has designed,
     * and `activate` already exists for the one case where bringing a row back is intended.
     */
    const retimable = [STATUS.Pending, STATUS.WaitingExecutionApproval];
    if (!retimable.includes(row.status)) {
      return json(409, {
        error: `cannot retime a schedule that is ${row.status}`,
        retimable,
      });
    }

    const triggerAt = new Date(body.triggerAt).toISOString();

    // `retimePatch` re-anchors a recurring chain; see its note for why that is not optional.
    const updated = await transition(
      row.id,
      row.status,
      row.status,
      retimePatch(row, triggerAt)
    );
    if (!updated) return json(409, { error: 'status changed while retiming' });

    // A parked row has no timer and must not get one — that is what `activate` is for.
    const timer =
      updated.status === STATUS.Pending
        ? await updateSchedule(updated)
        : { firesAt: null };

    const clamped = timer.firesAt !== null && timer.firesAt !== triggerAt;

    log('api', clamped ? 'retimedIntoThePast' : 'retimed', {
      scheduleId: row.id,
      from: row.triggerAt,
      to: triggerAt,
      firesAt: timer.firesAt,
    });

    return json(200, {
      scheduleId: row.id,
      status: updated.status,
      triggerAt,
      firesAt: timer.firesAt,
      // Said plainly rather than left for the caller to compare: a time already gone cannot be
      // given to `at()`, so the fire happens at the earliest moment Scheduler accepts instead.
      clamped,
    });
  },

  'POST /schedule/cancel': async ({ body, log }) => {
    const ids = Array.isArray(body?.scheduleIds) ? body.scheduleIds : [];
    if (ids.length === 0) return badRequest(['scheduleIds must be a non-empty array']);

    const cancelled = [];
    for (const id of ids) {
      const row = await getRow(id);
      if (!row) continue;
      // Only a pending row can be cancelled — an executed one has already fired and a failed one
      // is terminal. Same guard the policy-wide cancel used, just addressed by id.
      const updated = await transition(id, STATUS.Pending, STATUS.Cancelled);
      if (!updated) continue;
      await deleteSchedule(id);
      cancelled.push(id);
      log('api', 'cancelled', { scheduleId: id });
    }

    return json(200, { cancelled });
  },

  'POST /schedule/activate': async ({ body, log }) =>
    activate(body?.scheduleIds, log),

  'GET /v1/health': async () =>
    json(200, { service: 'schedule-platform-poc', status: 'ok' }),

  /**
   * GET /v1/schedule — note `policyId` is only a required string here, not a uuid, and no ACL
   * middleware guards this route in the old service. Both kept.
   */
  'GET /v1/schedule': async ({ query }) => {
    const errors = validateGet(query);
    if (errors.length) return badRequest(errors);

    const requested = splitStatuses(query.statuses);
    // No statuses given means pending only — the old processor's default condition.
    const statuses = requested.length > 0 ? requested : [STATUS.Pending];

    const rows = await queryByPolicy(query.policyId);
    return json(
      200,
      rows.filter((row) => statuses.includes(row.status))
    );
  },

  'POST /v1/schedule': async ({ body, log }) => {
    const errors = validateCreateV1(body);
    if (errors.length) return badRequest(errors);
    return create(body, { v2: false }, log);
  },

  'POST /v2/schedule': async ({ body, log }) => {
    const errors = validateCreateV2(body);
    if (errors.length) return badRequest(errors);
    return create(body, { v2: true }, log);
  },

  /**
   * POST /v1/schedule/cancel — `commands` says what to cancel, every other body key is a criterion
   * matched against `context.<key>`. A body carrying only `commands` is rejected, which is the old
   * handler's own guard against cancelling across every policy at once.
   */
  'POST /v1/schedule/cancel': async ({ body, log }) => {
    const errors = validateCancel(body);
    if (errors.length) return badRequest(errors);

    const { commands, ...criteria } = body;
    if (Object.keys(criteria).length === 0) {
      return json(400, {
        message: 'no criteria found for scheduled action cancellation',
      });
    }

    // A criterion is usually `policyId`, which the index serves. Anything else falls back to a
    // scan — fine for a PoC's row count.
    // ponytail: full scan on non-policy criteria. The real implementation needs either a criteria
    // index or a documented restriction to policyId; DPT-10343 should decide which.
    const candidates =
      typeof criteria.policyId === 'string'
        ? await queryByPolicy(criteria.policyId)
        : await scanRows();

    const matched = candidates.filter(
      (row) =>
        row.status === STATUS.Pending &&
        Object.entries(criteria).every(
          ([key, value]) => row.context?.[key] === value
        ) &&
        // Matched on `context.command` *or* `context.name`. The second is back-compat the old
        // processor still carries, and dropping it would silently stop cancelling older rows.
        (commands.includes(row.context?.command) ||
          commands.includes(row.context?.name))
    );

    const cancelled = await cancelRows(matched, log);
    log('api', 'cancelled', { count: cancelled.length, commands });
    return json(200, {});
  },

  /** DELETE /v2/schedule?policyId — cancels every pending row for one policy. */
  'DELETE /v2/schedule': async ({ query, log }) => {
    const errors = validateCancelPolicy(query);
    if (errors.length) return badRequest(errors);

    const rows = await queryByPolicy(query.policyId);
    const matched = rows.filter((row) => row.status === STATUS.Pending);
    const cancelled = await cancelRows(matched, log);

    log('api', 'policyCancelled', {
      policyId: query.policyId,
      count: cancelled.length,
    });
    return json(200, {});
  },

  /**
   * POST /v1/schedule/activate — releases rows parked at `waitingExecutionApproval` by
   * `context.__waitForApproval`, and mints the timer the parked row never had.
   *
   * The response string is reproduced verbatim, spaces included, because a caller may be matching
   * on it.
   */
  'POST /v1/schedule/activate': async ({ body, log }) => {
    const errors = validateActivate(body);
    if (errors.length) return badRequest(errors);
    return activate(body.scheduledActionIds, log);
  },

  // -------------------------------------------------------------------------------------------
  // Demo surface. Not part of the contract being replaced, and not something the real function
  // would carry — it exists so a room full of people can watch the platform work.
  // -------------------------------------------------------------------------------------------

  'GET /_debug/state': async ({ log }) => {
    // The two target queues drain into ONE feed row, so they must not run concurrently: each drain
    // reads the feed, prepends, and writes it back, and in parallel both would read the same
    // starting point and the later write would drop the other's messages. Losing a fired message
    // from the display mid-demo is the one bug here nobody would be able to explain on the spot.
    // The delivered text is the log line's own label, deliberately: what a room wants to read is
    // the message coming out the far end, not the word "received" with the message buried in a
    // detail blob beside it.
    const consumed = (entry) =>
      log(
        'consumer',
        entry.body?.payload?.message ?? entry.body?.topicName ?? 'received',
        { scheduleId: entry.body?.payload?.scheduleId }
      );

    await drainQueue(env.queueUrl, FEED_FIRED, consumed);
    const fired = await drainQueue(env.fifoQueueUrl, FEED_FIRED, consumed);

    // Depth first: `drainQueue` empties the queue, so asking afterwards always answers zero.
    const dlqDepth = await queueDepth(env.dlqUrl);

    const [rows, schedules, dlq, config, logs] = await Promise.all([
      scanRows(),
      listSchedules(),
      drainQueue(env.dlqUrl, FEED_DLQ, (entry) =>
        log('consumer', 'deadLetterCaptured', {
          scheduleId: entry.body?.requestPayload?.scheduleId,
        })
      ),
      getConfig(),
      readLogs(),
    ]);

    return json(200, {
      now: new Date().toISOString(),
      config,
      limits: {
        maxDeliveryAttempts: config.maxDeliveryAttempts ?? env.maxDeliveryAttempts,
        invocationsPerFiring: env.invocationsPerFiring,
        processingTtlMinutes: env.processingTtlMinutes,
        minLeadSeconds: env.minLeadSeconds,
      },
      rows: rows.sort((a, b) => String(a.triggerAt).localeCompare(b.triggerAt)),
      schedules,
      fired,
      dlq,
      dlqDepth,
      logs,
    });
  },

  'POST /_debug/config': async ({ body, log }) => {
    // Clamped rather than validated into an error: this is a demo control, and a cap of 40 on a
    // shared stack means a broken target loops for forty firings while everyone watches.
    const cap = Math.min(
      5,
      Math.max(1, Math.trunc(Number(body?.maxDeliveryAttempts)) || 1)
    );

    /**
     * Only what the body actually names. Building a full patch with defaults for the rest meant a
     * call setting one field silently cleared the others — a `curl` that changed the cap turned
     * `break publish` off, which during a demo looks like the platform recovering on its own.
     * The console sends all four, so nothing there changes.
     */
    const patch = {};
    for (const key of ['breakTarget', 'breakNext', 'legacyChain']) {
      if (body?.[key] !== undefined) patch[key] = Boolean(body[key]);
    }
    if (body?.maxDeliveryAttempts !== undefined) patch.maxDeliveryAttempts = cap;

    const config = await setConfig(patch);
    log('debug', 'configChanged', config);
    return json(200, config);
  },

  /** Deletes a timer without touching its row — the reconciler is supposed to notice and repair. */
  'POST /_debug/orphan': async ({ body, log }) => {
    const deleted = await deleteSchedule(body?.scheduleId);
    log('debug', 'timerDeletedBehindTheApp', {
      scheduleId: body?.scheduleId,
      deleted,
    });
    return json(200, { deleted });
  },

  /**
   * Deletes a row and leaves its timer standing — the mirror of /_debug/orphan.
   *
   * Two different repairs, and both need showing: a timer with no row must be deleted rather than
   * fired, and a fire that arrives for a deleted row must publish nothing.
   */
  'POST /_debug/orphan-row': async ({ body, log }) => {
    await deleteRow(body?.scheduleId);
    log('debug', 'rowDeletedBehindTheTimer', { scheduleId: body?.scheduleId });
    return json(200, { deleted: true });
  },

  'POST /_debug/reconcile': async ({ log }) => json(200, await reconcile(log)),

  'POST /_debug/reset': async ({ log }) => {
    const [rows, names] = await Promise.all([scanRows(), listSchedules()]);
    for (const row of rows) {
      await deleteSchedule(row.id);
      await deleteRow(row.id);
    }
    // CONFIG_ID goes too. `scanRows` filters out every `__`-prefixed id, so a reset that only
    // walked those rows left the demo switches set — and a `break publish` left on from an earlier
    // scenario then silently poisons the next one, which is a genuinely hard failure to read.
    // A button called "reset everything" resets everything.
    for (const id of [FEED_FIRED, FEED_DLQ, CONFIG_ID]) await deleteRow(id);

    // Drain the queues too. Deleting the feed rows without emptying the queues leaves messages
    // from before the reset to reappear on the next poll, which during a demo looks exactly like
    // a fire that just happened.
    const drained = [];
    for (const url of [env.queueUrl, env.fifoQueueUrl, env.dlqUrl]) {
      drained.push((await drainQueue(url, '__discard')).length);
    }
    await deleteRow('__discard');

    log('debug', 'reset', { rows: rows.length, schedules: names.length, drained });
    return json(200, { rows: rows.length, drained });
  },
};

// ---------------------------------------------------------------------------------------------
// Shared route helpers
// ---------------------------------------------------------------------------------------------

async function create(items, options, log) {
  const created = [];

  for (const item of items) {
    const row = newRow(item, options);
    await putRow(row);

    // A row parked for approval gets no timer. Minting one would fire it before anybody approved,
    // which is the whole point of the parked status.
    if (row.status === STATUS.Pending) await createSchedule(row);

    created.push({ id: row.id, status: row.status, triggerAt: row.triggerAt });
    log('api', 'created', {
      scheduleId: row.id,
      status: row.status,
      period: row.period,
    });
  }

  return json(201, { created });
}

/**
 * Release rows by id, and mint the timer a parked row never had.
 *
 * Unconditional on the current status, which is what the old handler did — it updates by
 * `id IN (...)` with no status predicate, so activating an already-cancelled row resurrects it.
 * Reproduced rather than corrected, and shared by both routes so the two cannot drift.
 */
async function activate(ids, log) {
  if (!Array.isArray(ids)) return badRequest(['ids must be an array']);

  let affectedCount = 0;
  for (const id of ids) {
    const row = await forceStatus(id, STATUS.Pending);
    if (!row?.id) continue;
    affectedCount += 1;
    await createSchedule(row);
    log('api', 'activated', { scheduleId: id, triggerAt: row.triggerAt });
  }

  return json(200, {
    message: `Schedule Action items activated, affected count : ${affectedCount}`,
  });
}

/** Cancel is two writes that must both happen: the row is the truth, the timer is the side effect. */
async function cancelRows(rows, log) {
  const cancelled = [];

  for (const row of rows) {
    const updated = await transition(row.id, STATUS.Pending, STATUS.Cancelled);
    if (!updated) continue;
    await deleteSchedule(row.id);
    cancelled.push(row.id);
    log('api', 'rowCancelled', { scheduleId: row.id });
  }

  return cancelled;
}
