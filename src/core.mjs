/**
 * Shared core for the schedule-platform PoC: the store, the recurrence maths, the inbound
 * validation and the outbound message envelope.
 *
 * Everything here is a deliberate port of `services/schedule`, not a redesign. Where the old
 * service does something surprising, this file reproduces the surprise and says so in a comment —
 * the point of the PoC is to prove the replacement behaves identically, and a quietly "improved"
 * recurrence or endAt boundary would prove the opposite.
 */

import { randomUUID } from 'node:crypto';

import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  ScanCommand,
  TransactWriteCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import {
  CreateScheduleCommand,
  DeleteScheduleCommand,
  GetScheduleCommand,
  ListSchedulesCommand,
  SchedulerClient,
  UpdateScheduleCommand,
} from '@aws-sdk/client-scheduler';
import {
  CloudWatchLogsClient,
  FilterLogEventsCommand,
} from '@aws-sdk/client-cloudwatch-logs';
import {
  DeleteMessageCommand,
  GetQueueAttributesCommand,
  ReceiveMessageCommand,
  SendMessageCommand,
  SQSClient,
} from '@aws-sdk/client-sqs';
import { DateTime, Duration } from 'luxon';

// ---------------------------------------------------------------------------------------------
// Constants copied from the service being replaced
// ---------------------------------------------------------------------------------------------

/**
 * All six values, not the four an outsider would guess. `preExecuted` and
 * `waitingExecutionApproval` are set by other services writing the same table, so the replacement
 * has to carry them even though nothing in the schedule service itself produces them.
 * Source: services/schedule/src/common/ScheduledActionStatus.ts
 */
export const STATUS = Object.freeze({
  Pending: 'pending',
  Executed: 'executed',
  Cancelled: 'cancelled',
  PreExecuted: 'preExecuted',
  Processing: 'processing',
  WaitingExecutionApproval: 'waitingExecutionApproval',
  /**
   * From the design, not from this PoC: docs/consistency.md has the dispatcher park a row here after
   * a ceiling of exhausted firings, and terraform#608 indexes `byStatus` for "admin queries for
   * `failed`". What the service being replaced lacks is the cap itself —
   * `resetLockedScheduledActions` returns a stuck row to `pending` for ever, so a permanently broken
   * target is retried until a human notices.
   */
  Failed: 'failed',
});

/** Character-for-character the regex in both create validation schemas. */
export const PERIOD_REGEX =
  /^(-?)P(?=\d|T\d)(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)([DW]))?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+(?:\.\d+)?)S)?)?$/;

const UUID_REGEX =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** services/schedule/src/common/config.ts */
const APP_KEY = 'schedule-service';
/** ACL_ALLOW_ALL_PERMISSION_STRING from @alteos-gmbh/common, resolved from the installed package. */
const PERMISSIONS = [
  '{"version":"1","action":"*","conditions":{},"internalResourceFields":[]}',
];
/** AuthorizationRole.AlteosService from @alteos-gmbh/acl.express. */
const ROLE_ALTEOS_SERVICE = 'alteosService';

/** Command values from @alteos-gmbh/dictionaries, needed by the processObligation special case. */
export const COMMAND = Object.freeze({
  ProcessObligation: 'processObligation',
  ConcludePolicy: 'concludePolicy',
});

export const CONFIG_ID = '__config';

// ---------------------------------------------------------------------------------------------
// Environment
// ---------------------------------------------------------------------------------------------

export const env = Object.freeze({
  table: required('POC_TABLE'),
  group: required('POC_SCHEDULE_GROUP'),
  queueUrl: required('POC_QUEUE_URL'),
  fifoQueueUrl: required('POC_FIFO_QUEUE_URL'),
  dlqUrl: required('POC_DLQ_URL'),
  schedulerRoleArn: required('POC_SCHEDULER_ROLE_ARN'),
  functionArn: required('POC_FUNCTION_ARN'),
  processingTtlMinutes: Number(process.env.POC_PROCESSING_TTL_MINUTES ?? '5'),
  /**
   * How far ahead a timer must be. Only a `triggerAt` closer than this gets pushed out, and the
   * only reason the floor exists at all is repair: the reconciler re-creates timers for occurrences
   * whose own time has already passed, and `at()` in the past is not a shape Scheduler documents.
   *
   * Small by default so a sub-minute `period` — `PT15S`, say — behaves as written. Raise it if a
   * demo needs the clamp to be visible.
   */
  logGroup: process.env.POC_LOG_GROUP ?? '',
  /**
   * How many times the platform will try to deliver one occurrence before giving up on it.
   *
   * A delivery is one firing, and each firing is already three Lambda invocations — the initial one
   * plus `maximum_retry_attempts = 2` — ending in one dead-letter record. So this counts firings,
   * not invocations. Confusing the two is easy: equal numbers hid the difference for a while during
   * the DPT-10338 spike, and `attempts` on the row deliberately counts the other thing.
   */
  maxDeliveryAttempts: Number(process.env.POC_MAX_DELIVERY_ATTEMPTS ?? '1'),
  /**
   * Invocations in one firing: Lambda's `maximum_retry_attempts` plus the first call.
   *
   * The function needs this because Lambda does not tell it which retry it is on — there is no
   * attempt index in the event or the context. So the row's own `attempts` is the counter, and this
   * is where it ends. Getting it wrong in either direction is visible: too low and the row is
   * parked before the dead-letter record exists, too high and it is never parked at all.
   */
  invocationsPerFiring: Number(process.env.POC_INVOCATIONS_PER_FIRING ?? '3'),
});

function required(name) {
  const value = process.env[name];
  if (!value) throw new Error(`missing environment variable ${name}`);
  return value;
}

const ddb = DynamoDBDocumentClient.from(new DynamoDBClient({}), {
  marshallOptions: { removeUndefinedValues: true },
});
const scheduler = new SchedulerClient({});
const sqs = new SQSClient({});
const cwl = new CloudWatchLogsClient({});

// ---------------------------------------------------------------------------------------------
// Store
// ---------------------------------------------------------------------------------------------

export async function getRow(id) {
  const { Item } = await ddb.send(
    new GetCommand({ TableName: env.table, Key: { id } })
  );
  return Item;
}

export async function putRow(row) {
  await ddb.send(new PutCommand({ TableName: env.table, Item: row }));
  return row;
}

/**
 * The status transition, guarded by a condition on the current value.
 *
 * This is what replaces the Redis lock (`locks:schedule:execute`, TTL 10s) the old service takes
 * around its batch claim. A conditional write is the same guarantee from the store itself, so the
 * PoC needs no cache at all — one fewer moving part in the replacement.
 */
export async function transition(id, from, to, extra = {}) {
  const names = { '#status': 'status', '#updatedAt': 'updatedAt' };
  const values = { ':to': to, ':updatedAt': new Date().toISOString() };
  const sets = ['#status = :to', '#updatedAt = :updatedAt'];

  // An undefined value must never reach the expression. Naming a placeholder in the SET clause and
  // then leaving it out of ExpressionAttributeValues is a ValidationException, so the value decides
  // whether the assignment exists at all — and the guard sits here rather than at each of the four
  // call sites, none of which currently passes undefined but any of which easily could.
  for (const [key, value] of Object.entries(extra)) {
    if (value === undefined) continue;
    names[`#${key}`] = key;
    values[`:${key}`] = value;
    sets.push(`#${key} = :${key}`);
  }

  let condition;
  if (Array.isArray(from)) {
    const placeholders = from
      .filter((value) => value !== undefined)
      .map((value, index) => {
        values[`:from${index}`] = value;
        return `:from${index}`;
      });
    if (placeholders.length === 0) {
      throw new Error('transition needs at least one from-status');
    }
    condition = `#status IN (${placeholders.join(', ')})`;
  } else {
    values[':from'] = from;
    condition = '#status = :from';
  }

  try {
    const { Attributes } = await ddb.send(
      new UpdateCommand({
        TableName: env.table,
        Key: { id },
        UpdateExpression: `SET ${sets.join(', ')}`,
        ConditionExpression: condition,
        ExpressionAttributeNames: names,
        ExpressionAttributeValues: values,
        ReturnValues: 'ALL_NEW',
      })
    );
    return Attributes;
  } catch (error) {
    if (error.name === 'ConditionalCheckFailedException') return undefined;
    throw error;
  }
}

export async function queryByPolicy(policyId) {
  const { Items = [] } = await ddb.send(
    new QueryCommand({
      TableName: env.table,
      IndexName: 'byPolicy',
      KeyConditionExpression: 'policyId = :policyId',
      ExpressionAttributeValues: { ':policyId': policyId },
    })
  );
  return Items;
}

export async function queryByStatus(status) {
  const { Items = [] } = await ddb.send(
    new QueryCommand({
      TableName: env.table,
      IndexName: 'byStatus',
      KeyConditionExpression: '#status = :status',
      ExpressionAttributeNames: { '#status': 'status' },
      ExpressionAttributeValues: { ':status': status },
    })
  );
  return Items;
}

export async function scanRows() {
  const { Items = [] } = await ddb.send(
    new ScanCommand({ TableName: env.table })
  );
  return Items.filter((item) => !String(item.id).startsWith('__'));
}

/**
 * The PoC's own demo switches, kept in the table so the UI can flip them with no redeploy.
 *
 * `maxDeliveryAttempts` lives here rather than only in the environment because changing it is part
 * of the demo — showing one firing and then showing two is the whole point of the setting, and a
 * Terraform apply between the two would break the thread of the argument. The environment variable
 * is the default this falls back to, so a reset returns to whatever the stack was deployed with.
 */
export async function getConfig() {
  const item = (await getRow(CONFIG_ID)) ?? {};
  return {
    breakTarget: false,
    breakNext: false,
    legacyChain: false,
    maxDeliveryAttempts: env.maxDeliveryAttempts,
    ...item,
  };
}

export async function setConfig(patch) {
  const current = await getConfig();
  const next = { ...current, ...patch, id: CONFIG_ID };
  await putRow(next);
  return next;
}

// ---------------------------------------------------------------------------------------------
// Recurrence — the part that must not drift from the old service
// ---------------------------------------------------------------------------------------------

/**
 * Line-for-line services/schedule/src/schedule/processors/calculateNextTriggerDate.ts.
 *
 * Two modes, and the difference matters: with `processingData` the next date is
 * `beginAt + period × (counter + 1)`, so a chain of twelve monthly fires lands on the same day of
 * the month every time. Without it the period is added to the *last* trigger, which accumulates
 * every rounding a month-length change introduces. Same luxon version does the arithmetic, so a
 * `P1M` added to 31 January clamps exactly as production clamps it.
 */
export function calculateNextTriggerDate(row) {
  const { period, triggerAt, processingData } = row;

  if (processingData !== undefined && processingData !== null) {
    const { beginAt, counter } = processingData;
    const duration = Duration.fromISO(period).mapUnits(
      (x) => x * (counter + 1)
    );
    return DateTime.fromISO(String(beginAt)).plus(duration).toISO() ?? '';
  }

  const duration = Duration.fromISO(period);
  return DateTime.fromISO(String(triggerAt)).plus(duration).toISO() ?? '';
}

/**
 * The `endAt` gate, reproduced including its day granularity.
 *
 * The old service compares `endAt.startOf('day') > next.endOf('day')`, so an `endAt` falling on the
 * same calendar day as the next trigger stops the chain even when the clock time would allow one
 * more fire. Written out rather than tidied, because a caller's chain length depends on it.
 */
export function shouldCreateNext(endAt, nextTriggerAt) {
  if (endAt === undefined || endAt === null) return true;
  return (
    DateTime.fromISO(String(endAt)).startOf('day') >
    DateTime.fromISO(String(nextTriggerAt)).endOf('day')
  );
}

// ---------------------------------------------------------------------------------------------
// Outbound envelope
// ---------------------------------------------------------------------------------------------

export const isFifo = (topicName) =>
  typeof topicName === 'string' && topicName.endsWith('.fifo');

/** A row with no `messageTopicName` is a V2 row and carries its own `message` verbatim. */
export const isV2Row = (row) =>
  row.messageTopicName === undefined || row.messageTopicName === null;

/**
 * V1 envelope. The schedule service mints the caller's authorization itself — no inbound token is
 * carried through the wait — and drops `partnerId` out of the context in the process, moving it to
 * `scopePartnerId`. Anything consuming a fired V1 schedule depends on this exact shape.
 */
export function buildV1Message(row) {
  const { partnerId, ...rest } = row.context ?? {};

  return {
    topicName: row.messageTopicName,
    payload: {
      authorizationData: {
        appKey: APP_KEY,
        userId: null,
        partnerId: null,
        customerId: null,
        agentId: null,
        permissions: PERMISSIONS,
        testingFlags: [],
        roles: [ROLE_ALTEOS_SERVICE],
        scopePartnerId: partnerId ?? null,
        traceIds: {},
        requestId: randomUUID(),
      },
      ...rest,
    },
    messageGroupId: isFifo(row.messageTopicName)
      ? row.context?.policyId
      : undefined,
  };
}

/**
 * Publish, standing in for `MessageBrokerClient`. A FIFO topic name routes to the FIFO queue with
 * `messageGroupId` set, which is how the old service preserves per-policy ordering; everything else
 * goes to the standard queue.
 */
export async function publish({ topicName, payload, messageGroupId }) {
  const fifo = isFifo(topicName);
  await sqs.send(
    new SendMessageCommand({
      QueueUrl: fifo ? env.fifoQueueUrl : env.queueUrl,
      MessageBody: JSON.stringify({ topicName, payload }),
      ...(fifo
        ? {
            MessageGroupId: messageGroupId ?? 'default',
            MessageDeduplicationId: randomUUID(),
          }
        : {}),
    })
  );
}

// ---------------------------------------------------------------------------------------------
// EventBridge Scheduler — the timer, and only the timer
// ---------------------------------------------------------------------------------------------

export const scheduleNameFor = (id) => `poc-schedule-${id}`;

/**
 * One one-shot schedule per pending occurrence.
 *
 * `ConflictException` is answered rather than thrown: a repeated CreateSchedule under an existing
 * name does not overwrite, so the conflict is proof the first attempt landed. Measured on
 * 07.09.2026 — see docs/consistency.md. A `ClientToken` changes nothing here and is not sent.
 */
export async function createSchedule(row) {
  const input = scheduleDefinition(row);

  try {
    await scheduler.send(new CreateScheduleCommand(input));
    return { created: true, name: input.Name };
  } catch (error) {
    if (error.name === 'ConflictException') return { created: false, name: input.Name };
    throw error;
  }
}

/**
 * Move an existing timer to the row's current `triggerAt`.
 *
 * `UpdateSchedule` is a full replace rather than a merge, so the whole definition goes back every
 * time — the design's README flags this for whoever writes `PATCH`, and the reason it is not a
 * problem here is that this function owns the definition and never has to read one first.
 *
 * A missing timer falls through to a create. That happens when the row is `pending` but its timer
 * was lost, or when a parked row is being retimed before it ever had one — a retime that failed
 * because there was nothing to update would be a strange way to learn that.
 */
export async function updateSchedule(row) {
  const input = scheduleDefinition(row);

  try {
    await scheduler.send(new UpdateScheduleCommand(input));
    return { updated: true, name: input.Name };
  } catch (error) {
    if (error.name !== 'ResourceNotFoundException') throw error;
    return { updated: false, ...(await createSchedule(row)) };
  }
}

/**
 * The full one-shot definition for a row.
 *
 * The row's own `triggerAt` is used verbatim, including a time that has already gone.
 *
 * An earlier version pushed a past time forward to `now + a lead`, on the assumption that
 * `at()` in the past was not a shape Scheduler accepts. Measured 10.09.2026 and the assumption was
 * wrong: `at()` an hour in the past is accepted, and a schedule two minutes in the past invoked the
 * function about 45 seconds after being created — the same latency as any other fire. So the lead
 * was doing nothing except delaying a repair by ten seconds and rewriting what the caller asked
 * for. A retime aimed backwards now fires as soon as Scheduler gets to it, which is what anyone
 * moving a trigger into the past means by it.
 */
function scheduleDefinition(row) {
  const when = DateTime.fromISO(String(row.triggerAt)).toUTC();

  // Second precision, formatted explicitly. Scheduler rejects an `at()` carrying fractional
  // seconds — `Invalid Schedule Expression at(2026-09-08T03:54:26.439)` — and luxon's
  // `suppressMilliseconds` only drops them when they happen to be zero, so it is not a fix.
  return {
    Name: scheduleNameFor(row.id),
    GroupName: env.group,
    ScheduleExpression: `at(${when.toFormat("yyyy-MM-dd'T'HH:mm:ss")})`,
    ScheduleExpressionTimezone: 'UTC',
    FlexibleTimeWindow: { Mode: 'OFF' },
    ActionAfterCompletion: 'DELETE',
    Target: {
      Arn: env.functionArn,
      RoleArn: env.schedulerRoleArn,
      Input: JSON.stringify({ scheduleId: row.id }),
    },
  };
}

export async function deleteSchedule(id) {
  try {
    await scheduler.send(
      new DeleteScheduleCommand({
        Name: scheduleNameFor(id),
        GroupName: env.group,
      })
    );
    return true;
  } catch (error) {
    if (error.name === 'ResourceNotFoundException') return false;
    throw error;
  }
}

export async function scheduleExists(id) {
  try {
    await scheduler.send(
      new GetScheduleCommand({
        Name: scheduleNameFor(id),
        GroupName: env.group,
      })
    );
    return true;
  } catch (error) {
    if (error.name === 'ResourceNotFoundException') return false;
    throw error;
  }
}

export async function listSchedules() {
  const names = [];
  let token;
  do {
    const page = await scheduler.send(
      new ListSchedulesCommand({
        GroupName: env.group,
        NextToken: token,
        MaxResults: 100,
      })
    );
    for (const item of page.Schedules ?? []) names.push(item.Name);
    token = page.NextToken;
  } while (token);
  return names;
}

// ---------------------------------------------------------------------------------------------
// Inbound validation — ported from the Joi schemas, same fields, same required-ness
// ---------------------------------------------------------------------------------------------

const isIsoDate = (value) =>
  typeof value === 'string' && DateTime.fromISO(value).isValid;
const isUuid = (value) => typeof value === 'string' && UUID_REGEX.test(value);
const isPlainObject = (value) =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

/**
 * POST /schedule — the dumb-service shape: when to fire, how often, and what to say.
 *
 * No policy, no partner, no command, no topic. This PoC exists to show the platform mechanics —
 * the store as the source of truth, the timer as only a timer, and what retry, dead-lettering and
 * reconciliation do — and the domain fields were noise in front of that. The ported inbound surface
 * of the old service still lives on the `/v1` and `/v2` routes and in the demo scripts; nothing
 * here removes it.
 */
export function validateCreateSimple(body) {
  const errors = [];
  if (!isPlainObject(body)) return ['body must be an object'];

  if (!isIsoDate(body.triggerAt)) errors.push('triggerAt must be an ISO date');
  if (body.period !== undefined && body.period !== null && !PERIOD_REGEX.test(String(body.period)))
    errors.push('period must be an ISO 8601 duration');
  if (typeof body.message !== 'string' || body.message.trim() === '')
    errors.push('message is required');
  if (typeof body.message === 'string' && body.message.length > 500)
    errors.push('message must be 500 characters or fewer');

  return errors;
}

/** POST /v1/schedule — an array, and `context` needs command/partnerId/policyId. */
export function validateCreateV1(body) {
  const errors = [];
  if (!Array.isArray(body)) return ['body must be an array'];

  body.forEach((item, index) => {
    const at = (message) => errors.push(`[${index}] ${message}`);
    if (!isPlainObject(item)) return at('must be an object');
    if (!isIsoDate(item.triggerAt)) at('triggerAt must be an ISO date');
    if (item.endAt !== undefined && !isIsoDate(item.endAt))
      at('endAt must be an ISO date');
    if (item.period !== undefined && !PERIOD_REGEX.test(String(item.period)))
      at('period must be an ISO 8601 duration');
    if (typeof item.messageTopicName !== 'string' || item.messageTopicName === '')
      at('messageTopicName is required');
    if (!isPlainObject(item.context)) return at('context is required');
    // `command` is a plain string here, not required — matches baseContextSchema.
    if (item.context.command !== undefined && typeof item.context.command !== 'string')
      at('context.command must be a string');
    if (!isUuid(item.context.partnerId)) at('context.partnerId must be a uuid');
    if (!isUuid(item.context.policyId)) at('context.policyId must be a uuid');
  });

  return errors;
}

/** POST /v2/schedule — `message` and `context` are opaque objects, both required. */
export function validateCreateV2(body) {
  const errors = [];
  if (!Array.isArray(body)) return ['body must be an array'];

  body.forEach((item, index) => {
    const at = (message) => errors.push(`[${index}] ${message}`);
    if (!isPlainObject(item)) return at('must be an object');
    if (!isPlainObject(item.message)) at('message is required');
    if (!isIsoDate(item.triggerAt)) at('triggerAt must be an ISO date');
    if (item.endAt !== undefined && !isIsoDate(item.endAt))
      at('endAt must be an ISO date');
    if (item.period !== undefined && !PERIOD_REGEX.test(String(item.period)))
      at('period must be an ISO 8601 duration');
    if (!isPlainObject(item.context)) at('context is required');
  });

  return errors;
}

/**
 * POST /v1/schedule/cancel — `commands` is required, and every *other* key in the body is a
 * criterion matched against `context.<key>`. The schema allows unknown keys deliberately, so a
 * validator that rejected them would break every caller.
 */
export function validateCancel(body) {
  if (!isPlainObject(body)) return ['body must be an object'];
  if (!Array.isArray(body.commands) || body.commands.length < 1)
    return ['commands must be a non-empty array'];
  if (!body.commands.every((command) => typeof command === 'string'))
    return ['commands must be strings'];
  return [];
}

export function validateActivate(body) {
  if (!isPlainObject(body)) return ['body must be an object'];
  if (!Array.isArray(body.scheduledActionIds))
    return ['scheduledActionIds must be an array'];
  if (!body.scheduledActionIds.every((id) => typeof id === 'string'))
    return ['scheduledActionIds must be strings'];
  return [];
}

/** GET /v1/schedule — note `policyId` is only `string().required()` here, not a uuid. */
export function validateGet(query) {
  const errors = [];
  if (typeof query.policyId !== 'string' || query.policyId === '')
    errors.push('policyId is required');
  // API Gateway collapses a repeated query parameter into one comma-joined value, so
  // `?statuses=pending&statuses=executed` arrives as the string "pending,executed". Splitting has
  // to happen before the enum check or every multi-status read is rejected.
  for (const status of splitStatuses(query.statuses))
    if (!Object.values(STATUS).includes(status))
      errors.push(`unknown status ${status}`);
  return errors;
}

export function validateCancelPolicy(query) {
  return isUuid(query.policyId) ? [] : ['policyId must be a uuid'];
}

export const splitStatuses = (value) =>
  asArray(value)
    .flatMap((item) => String(item).split(','))
    .map((item) => item.trim())
    .filter(Boolean);

/**
 * The row patch for a retime.
 *
 * Separated out and tested because it is the part that fails quietly. A recurring row computes its
 * next occurrence as `beginAt + period × (counter + 1)`, so moving `triggerAt` without moving
 * `beginAt` leaves the chain anchored to the time the schedule used to have: the retime looks
 * correct, and then the occurrence after it jumps back to the old rhythm. A one-shot row has no
 * anchor to move.
 */
export function retimePatch(row, triggerAt) {
  const recurring = row.period !== null && row.period !== undefined;
  return recurring
    ? { triggerAt, processingData: { beginAt: triggerAt, counter: 0 } }
    : { triggerAt };
}

/** Exported for the retime route, which validates one date rather than a whole body. */
export const validateIsoDate = (value) => isIsoDate(value);

export const asArray = (value) =>
  value === undefined || value === null
    ? []
    : Array.isArray(value)
      ? value
      : [value];

// ---------------------------------------------------------------------------------------------
// Row construction
// ---------------------------------------------------------------------------------------------

/**
 * `policyId` is lifted out of `context` onto the item so the byPolicy index has a key. The old
 * service queries `context.policyId` inside a JSONB column, which DynamoDB cannot index — this is
 * the one shape change the move to DynamoDB forces, and every read path uses the lifted copy.
 */
/**
 * A row for the dumb-service shape.
 *
 * `policyId` is still written because the `byPolicy` index needs a key, but it carries no meaning
 * any more — every simple row shares one value, and reads go through the id or the status sweep.
 * Removing the index would mean a Terraform change for nothing.
 */
export function newSimpleRow({ triggerAt, period, message }) {
  const now = new Date().toISOString();
  const at = DateTime.fromISO(String(triggerAt)).toUTC().toISO();
  const recurring = period !== undefined && period !== null && String(period) !== '';

  return {
    id: randomUUID(),
    policyId: SIMPLE_PARTITION,
    kind: 'simple',
    message: String(message),
    status: STATUS.Pending,
    period: recurring ? String(period) : null,
    triggerAt: at,
    processingData: recurring ? { beginAt: triggerAt, counter: 0 } : undefined,
    attempts: 0,
    deliveries: 1,
    createdAt: now,
    updatedAt: now,
  };
}

export const SIMPLE_PARTITION = 'poc';

export const isSimpleRow = (row) => row.kind === 'simple';

export function newRow(input, { v2 }) {
  const now = new Date().toISOString();
  const triggerAt = DateTime.fromISO(String(input.triggerAt)).toUTC().toISO();

  /**
   * `__waitForApproval` in the context parks the row at `waitingExecutionApproval` instead of
   * `pending`, which is what `POST /v1/schedule/activate` later releases. V1 only — the V2 handler
   * always creates `pending`, so the same flag in a V2 context is silently ignored by the service
   * being replaced. Reproduced, not corrected.
   */
  const waitForApproval = !v2 && input.context?.__waitForApproval === true;

  return {
    id: randomUUID(),
    policyId: input.context?.policyId ?? 'unknown',
    context: input.context ?? {},
    ...(v2
      ? { message: input.message }
      : { messageTopicName: input.messageTopicName }),
    status: waitForApproval ? STATUS.WaitingExecutionApproval : STATUS.Pending,
    period: input.period ?? null,
    triggerAt,
    endAt: input.endAt
      ? DateTime.fromISO(String(input.endAt)).toUTC().toISO()
      : undefined,
    /**
     * Both create handlers guard this with `item.period !== null`, and an absent `period` is
     * `undefined`, not `null` — so a one-shot action created without the key still gets a
     * `processingData` block. Harmless, and copied so a row written here is byte-comparable with a
     * row the old service would have written.
     */
    processingData:
      input.period !== null ? { beginAt: input.triggerAt, counter: 0 } : undefined,
    /** From the caller's authorizationData in the old service; the PoC runs no ACL, so from the item. */
    testingFlags: v2 ? undefined : input.testingFlags,
    attempts: 0,
    /** Firings used so far. The first one is this row's own, hence 1 rather than 0. */
    deliveries: 1,
    createdAt: now,
    updatedAt: now,
  };
}

// ---------------------------------------------------------------------------------------------
// The atomic chain step
// ---------------------------------------------------------------------------------------------

/**
 * Mark the fired occurrence `executed` and write the next one in a single transaction.
 *
 * This is the one place the PoC deliberately does NOT copy the old service, and the reason the
 * whole exercise exists. `executeScheduledActions` marks the row executed, *then* creates the next
 * occurrence, with both calls inside one `try/catch` that only logs. Anything that throws between
 * the two — a DB blip, or the `processObligation` branch reading
 * `concludePolicyScheduleAction[0].triggerAt` when no conclude schedule exists — leaves the row
 * `executed` and the chain permanently dead, visible as a single log line and nothing else.
 *
 * Both writes land or neither does, so there is no window in which the chain can end silently.
 */
export async function commitChainStep(row, next) {
  const now = new Date().toISOString();

  const items = [
    {
      Update: {
        TableName: env.table,
        Key: { id: row.id },
        UpdateExpression: 'SET #status = :executed, #updatedAt = :now',
        ConditionExpression: '#status = :processing',
        ExpressionAttributeNames: { '#status': 'status', '#updatedAt': 'updatedAt' },
        ExpressionAttributeValues: {
          ':executed': STATUS.Executed,
          ':processing': STATUS.Processing,
          ':now': now,
        },
      },
    },
  ];

  if (next) {
    items.push({
      Put: {
        TableName: env.table,
        Item: next,
        ConditionExpression: 'attribute_not_exists(id)',
      },
    });
  }

  await ddb.send(new TransactWriteCommand({ TransactItems: items }));
}

/** The old ordering, kept behind a switch so one system can demonstrate both. */
export async function commitChainStepLegacy(row, next, { breakNext }) {
  await transition(row.id, STATUS.Processing, STATUS.Executed);
  if (breakNext) {
    // Exactly what the old service's catch block does with a throw from createScheduledActions:
    // swallow it. The row is already `executed`, so nothing will ever fire for this policy again.
    return { swallowed: 'next occurrence write failed and was only logged' };
  }
  if (next) await putRow(next);
  return {};
}

// ---------------------------------------------------------------------------------------------
// The observable feeds
// ---------------------------------------------------------------------------------------------

export const FEED_FIRED = '__fired';
export const FEED_DLQ = '__dlq';

/** Keeps a demo readable without keeping a queue's worth of history in one item. */
const FEED_CAP = 40;

/**
 * Move whatever is on a queue into a feed row, so the UI has something durable to show.
 *
 * A queue is a bad display surface — a message read for the screen is gone for everyone else — so
 * each poll drains the queue once and appends to an item the UI can re-read as often as it likes.
 */
export async function drainQueue(queueUrl, feedId, notify) {
  const received = [];

  for (let round = 0; round < 3; round += 1) {
    const { Messages = [] } = await sqs.send(
      new ReceiveMessageCommand({
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 10,
        WaitTimeSeconds: 0,
        MessageAttributeNames: ['All'],
      })
    );
    if (Messages.length === 0) break;

    for (const message of Messages) {
      const entry = { at: new Date().toISOString(), body: safeParse(message.Body) };
      received.push(entry);
      // The caller logs it. Draining is the only place a message is ever read, so this is the
      // closest thing the PoC has to a consumer, and a demo wants to see it happen.
      notify?.(entry);
      await sqs.send(
        new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle,
        })
      );
    }
  }

  if (received.length === 0) return readFeed(feedId);

  const existing = await readFeed(feedId);
  const items = [...received, ...existing].slice(0, FEED_CAP);
  await putRow({ id: feedId, items });
  return items;
}

export async function readFeed(feedId) {
  const row = await getRow(feedId);
  return row?.items ?? [];
}

function safeParse(body) {
  try {
    return JSON.parse(body);
  } catch {
    return { unparsed: String(body).slice(0, 2000) };
  }
}

export async function deleteRow(id) {
  await ddb.send(new DeleteCommand({ TableName: env.table, Key: { id } }));
}

/**
 * An unconditional status write, which is what `POST /v1/schedule/activate` does.
 *
 * The old handler updates by `id IN (...)` with no status predicate, so activating an id that is
 * already `cancelled` or `executed` puts it back to `pending` and it fires again. Reproduced
 * because a caller may well be relying on it; flagged because it is almost certainly not intended.
 */
export async function forceStatus(id, to) {
  const { Attributes } = await ddb.send(
    new UpdateCommand({
      TableName: env.table,
      Key: { id },
      UpdateExpression: 'SET #status = :to, #updatedAt = :now',
      ConditionExpression: 'attribute_exists(id)',
      ExpressionAttributeNames: { '#status': 'status', '#updatedAt': 'updatedAt' },
      ExpressionAttributeValues: { ':to': to, ':now': new Date().toISOString() },
      ReturnValues: 'ALL_NEW',
    })
  ).catch((error) => {
    if (error.name === 'ConditionalCheckFailedException') return {};
    throw error;
  });
  return Attributes;
}

// ---------------------------------------------------------------------------------------------
// The function's own logs, read back for the console
// ---------------------------------------------------------------------------------------------

/**
 * Recent log lines from this function's own log group.
 *
 * The console shows these so a demo can point at cause and effect in the same window — the row
 * changing status on the left, the line that changed it on the right. It reads its own logs rather
 * than the operator's machine reading CloudWatch, so the console keeps its single dependency and
 * works unchanged behind a tunnel.
 *
 * CloudWatch ingestion lags by seconds, so the panel is always slightly behind the table. That is
 * the display being late, not the platform.
 *
 * An hour of history rather than fifteen minutes: once the filtering moved server-side only handler
 * lines match, so the window costs almost nothing — and a fifteen-minute window meant that coming
 * back to ask "what happened to that row" half an hour later had no answer left.
 */
export async function readLogs({ minutes = 60, limit = 80 } = {}) {
  if (!env.logGroup) return [];

  try {
    const { events = [] } = await cwl.send(
      new FilterLogEventsCommand({
        logGroupName: env.logGroup,
        startTime: Date.now() - minutes * 60_000,
        /**
         * Filter server-side, and the reason matters. `FilterLogEvents` pages from the *oldest*
         * event in the window with no way to ask for the newest, and the console polls this
         * function every two seconds — so an unfiltered window of fifteen minutes is well over a
         * thousand runtime START/END/REPORT lines and the first page never reaches anything the
         * handler wrote.
         *
         * Every handler line is one JSON object carrying `component`, so matching that literal
         * leaves only those. A plain substring pattern rather than the JSON `{ $.component = * }`
         * form, because the runtime prefixes each line with a timestamp and request id and the
         * event is therefore not valid JSON on its own.
         */
        filterPattern: '"component"',
        limit: 200,
      })
    );

    return events
      .map((event) => shapeLogLine(event))
      .filter(Boolean)
      .sort((a, b) => b.at - a.at)
      .slice(0, limit);
  } catch (error) {
    // A missing permission or a log group that does not exist yet must not take the whole console
    // down with it — the panel says why instead.
    return [
      {
        at: Date.now(),
        kind: 'consoleError',
        text: `could not read ${env.logGroup}: ${error.name}`,
      },
    ];
  }
}

function shapeLogLine(event) {
  const raw = String(event.message ?? '').trim();
  if (!raw) return undefined;

  /**
   * The runtime's own framing, all of it dropped.
   *
   * REPORT looks tempting — duration and memory next to a fire — but the console polls this
   * function every two seconds, so each poll writes its own START/END/REPORT. Keeping them means
   * the panel is entirely the console watching itself, and the handful of lines that matter are
   * pushed off the end within a minute. Everything below this point is something the handler chose
   * to log.
   */
  if (
    raw.startsWith('START ') ||
    raw.startsWith('END ') ||
    raw.startsWith('REPORT ') ||
    raw.startsWith('INIT_START ')
  ) {
    return undefined;
  }

  // Anything the handler logged, which is always one JSON object per line.
  const json = raw.slice(raw.indexOf('{'));
  try {
    const parsed = JSON.parse(json);
    const { component, message, ...detail } = parsed;
    return {
      at: event.timestamp,
      kind: component ?? 'log',
      text: message ?? '',
      detail: Object.keys(detail).length > 0 ? detail : undefined,
    };
  } catch {
    return { at: event.timestamp, kind: 'raw', text: raw.slice(0, 300) };
  }
}

/**
 * How many messages are actually on a queue right now.
 *
 * Worth showing beside the feed, because the feed is not the queue. Draining is what lets a browser
 * poll the same dead letter every two seconds without consuming it from under anyone, but it also
 * means the real queue is empty within a poll of a record arriving — and someone opening SQS in the
 * console to check finds nothing there. Reporting both numbers stops the panel from implying the
 * messages are still in AWS.
 */
export async function queueDepth(queueUrl) {
  try {
    const { Attributes } = await sqs.send(
      new GetQueueAttributesCommand({
        QueueUrl: queueUrl,
        AttributeNames: ['ApproximateNumberOfMessages'],
      })
    );
    return Number(Attributes?.ApproximateNumberOfMessages ?? 0);
  } catch {
    return null;
  }
}
