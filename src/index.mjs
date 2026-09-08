/**
 * The schedule-platform PoC Lambda. One artifact, four ways in — the same split the scaffold in
 * services-monorepo#2561 established, and the guards below are that PR's guards.
 *
 * The seven management routes are the complete inbound surface of `services/schedule`, ported
 * route for route so a caller cutover needs no client change beyond the transport.
 */

import {
  createSchedule,
  deleteRow,
  deleteSchedule,
  drainQueue,
  env,
  FEED_DLQ,
  FEED_FIRED,
  forceStatus,
  getConfig,
  listSchedules,
  newRow,
  putRow,
  queryByPolicy,
  scanRows,
  setConfig,
  splitStatuses,
  STATUS,
  transition,
  validateActivate,
  validateCancel,
  validateCancelPolicy,
  validateCreateV1,
  validateCreateV2,
  validateGet,
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
    console.log(JSON.stringify({ component, message, ...detail }));
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

    let affectedCount = 0;
    for (const id of body.scheduledActionIds) {
      const row = await forceStatus(id, STATUS.Pending);
      if (!row?.id) continue;
      affectedCount += 1;
      await createSchedule(row);
      log('api', 'activated', { scheduleId: id, triggerAt: row.triggerAt });
    }

    return json(200, {
      message: `Schedule Action items activated, affected count : ${affectedCount}`,
    });
  },

  // -------------------------------------------------------------------------------------------
  // Demo surface. Not part of the contract being replaced, and not something the real function
  // would carry — it exists so a room full of people can watch the platform work.
  // -------------------------------------------------------------------------------------------

  'GET /_debug/state': async () => {
    // The two target queues drain into ONE feed row, so they must not run concurrently: each drain
    // reads the feed, prepends, and writes it back, and in parallel both would read the same
    // starting point and the later write would drop the other's messages. Losing a fired message
    // from the display mid-demo is the one bug here nobody would be able to explain on the spot.
    await drainQueue(env.queueUrl, FEED_FIRED);
    const fired = await drainQueue(env.fifoQueueUrl, FEED_FIRED);

    const [rows, schedules, dlq, config] = await Promise.all([
      scanRows(),
      listSchedules(),
      drainQueue(env.dlqUrl, FEED_DLQ),
      getConfig(),
    ]);

    return json(200, {
      now: new Date().toISOString(),
      config,
      rows: rows.sort((a, b) => String(a.triggerAt).localeCompare(b.triggerAt)),
      schedules,
      fired,
      dlq,
    });
  },

  'POST /_debug/config': async ({ body, log }) => {
    const config = await setConfig({
      breakTarget: Boolean(body?.breakTarget),
      breakNext: Boolean(body?.breakNext),
      legacyChain: Boolean(body?.legacyChain),
    });
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
    for (const id of [FEED_FIRED, FEED_DLQ]) await deleteRow(id);

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
