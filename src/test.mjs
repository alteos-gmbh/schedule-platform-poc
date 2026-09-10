/**
 * One runnable check for the logic a demo cannot prove by itself.
 *
 * The AWS paths are exercised by the demo against real services; what needs a test is the pure
 * arithmetic and validation ported from the old service, because a silent difference there is
 * exactly the kind of thing a live demo makes look fine.
 *
 *   node test.mjs
 */

import assert from 'node:assert/strict';

// The old service's endAt gate resolves startOf/endOf('day') in the *process* time zone: its
// `convertToDateTime` is a bare `DateTime.fromISO(date)` with no zone pinned. Deployed containers
// and Lambda both run UTC, so UTC is the behaviour under test. Set before luxon loads.
process.env.TZ = 'UTC';

// core.mjs reads its configuration at module scope, so a cold start fails loudly on a missing
// setting. The values are never used by anything under test here.
for (const name of [
  'POC_TABLE',
  'POC_SCHEDULE_GROUP',
  'POC_QUEUE_URL',
  'POC_FIFO_QUEUE_URL',
  'POC_DLQ_URL',
  'POC_SCHEDULER_ROLE_ARN',
  'POC_FUNCTION_ARN',
]) {
  process.env[name] = `test-${name}`;
}

const {
  buildV1Message,
  calculateNextTriggerDate,
  newRow,
  PERIOD_REGEX,
  shouldCreateNext,
  STATUS,
  validateCreateV1,
  validateCreateV2,
  validateCancel,
  validateCreateSimple,
  newSimpleRow,
} = await import('./core.mjs');

const tests = [];
const test = (name, fn) => tests.push([name, fn]);

// -----------------------------------------------------------------------------------------------
// Recurrence: the anti-drift chain is the reason processingData exists
// -----------------------------------------------------------------------------------------------

test('processingData anchors every occurrence to beginAt, so a monthly chain does not drift', () => {
  const row = {
    period: 'P1M',
    triggerAt: '2026-01-31T09:00:00.000Z',
    processingData: { beginAt: '2026-01-31T09:00:00.000Z', counter: 0 },
  };

  // January has 31 days and February does not, so the first hop clamps.
  const second = calculateNextTriggerDate(row);
  assert.equal(second.slice(0, 10), '2026-02-28');

  // The clamp must not become the new anchor: hop two is beginAt + P2M, back on the 31st.
  const third = calculateNextTriggerDate({
    ...row,
    triggerAt: second,
    processingData: { beginAt: row.processingData.beginAt, counter: 1 },
  });
  assert.equal(third.slice(0, 10), '2026-03-31');
});

test('without processingData the period is added to the last trigger, and the clamp sticks', () => {
  const second = calculateNextTriggerDate({
    period: 'P1M',
    triggerAt: '2026-01-31T09:00:00.000Z',
  });
  assert.equal(second.slice(0, 10), '2026-02-28');

  // Drift: the 31st is gone for good. Reproduced deliberately — a caller that omits processingData
  // gets this from the old service too.
  const third = calculateNextTriggerDate({ period: 'P1M', triggerAt: second });
  assert.equal(third.slice(0, 10), '2026-03-28');
});

// -----------------------------------------------------------------------------------------------
// The endAt gate, at day granularity
// -----------------------------------------------------------------------------------------------

test('endAt on the same calendar day as the next trigger ends the chain', () => {
  assert.equal(
    shouldCreateNext('2026-03-31T23:00:00.000Z', '2026-03-31T09:00:00.000Z'),
    false,
    'same day: startOf(day) is not greater than endOf(day)'
  );
});

test('endAt after the next trigger day allows one more occurrence', () => {
  assert.equal(
    shouldCreateNext('2026-04-01T00:00:00.000Z', '2026-03-31T09:00:00.000Z'),
    true
  );
});

test('the endAt gate is time-zone dependent, which nothing in the old service declares', async () => {
  // Same two instants, read in a UTC+7 zone: 23:00Z on the 31st is 06:00 on the 1st locally, so
  // startOf('day') lands a day later and one extra occurrence is created. A schedule service
  // container started with a non-UTC TZ would produce different chain lengths for the same input.
  // Worth naming in the RFC; the PoC pins TZ=UTC on the function so the behaviour is declared.
  const { DateTime } = await import('luxon');
  const inZone = (value) => DateTime.fromISO(value, { zone: 'Asia/Saigon' });
  const endAt = '2026-03-31T23:00:00.000Z';
  const next = '2026-03-31T09:00:00.000Z';

  assert.equal(shouldCreateNext(endAt, next), false, 'UTC: chain ends');
  assert.equal(
    inZone(endAt).startOf('day') > inZone(next).endOf('day'),
    true,
    'UTC+7: the same inputs allow one more occurrence'
  );
});

test('no endAt means the chain never ends on its own', () => {
  assert.equal(shouldCreateNext(undefined, '2026-03-31T09:00:00.000Z'), true);
  assert.equal(shouldCreateNext(null, '2026-03-31T09:00:00.000Z'), true);
});

// -----------------------------------------------------------------------------------------------
// The period regex, character-for-character the old one
// -----------------------------------------------------------------------------------------------

test('period regex accepts what the old schemas accept and rejects what they reject', () => {
  for (const good of ['P1M', 'P1Y2M3DT4H5M6S', 'P2W', '-P1D', 'PT30S', 'PT0.5S']) {
    assert.ok(PERIOD_REGEX.test(good), `${good} should be accepted`);
  }
  for (const bad of ['1M', 'P', '', 'PT', 'month', 'P1H']) {
    assert.ok(!PERIOD_REGEX.test(bad), `${bad} should be rejected`);
  }
});

// -----------------------------------------------------------------------------------------------
// Inbound validation
// -----------------------------------------------------------------------------------------------

const validV1 = () => [
  {
    triggerAt: '2026-10-01T09:00:00.000Z',
    period: 'P1M',
    messageTopicName: 'policy-obligations',
    context: {
      command: 'processObligation',
      partnerId: '11111111-1111-4111-8111-111111111111',
      policyId: '22222222-2222-4222-8222-222222222222',
    },
  },
];

test('a well-formed V1 create passes', () => {
  assert.deepEqual(validateCreateV1(validV1()), []);
});

test('V1 create requires messageTopicName and uuid partner/policy ids', () => {
  const [item] = validV1();
  assert.ok(
    validateCreateV1([{ ...item, messageTopicName: undefined }]).length > 0
  );
  assert.ok(
    validateCreateV1([
      { ...item, context: { ...item.context, partnerId: 'not-a-uuid' } },
    ]).length > 0
  );
  assert.ok(validateCreateV1({ not: 'an array' }).length > 0);
});

test('V2 create requires message and context and never messageTopicName', () => {
  assert.deepEqual(
    validateCreateV2([
      {
        message: { topicName: 'x', payload: {} },
        context: { policyId: 'anything' },
        triggerAt: '2026-10-01T09:00:00.000Z',
      },
    ]),
    []
  );
  assert.ok(
    validateCreateV2([
      { context: {}, triggerAt: '2026-10-01T09:00:00.000Z' },
    ]).length > 0
  );
});

test('cancel needs a non-empty commands array', () => {
  assert.deepEqual(validateCancel({ commands: ['concludePolicy'] }), []);
  assert.ok(validateCancel({ commands: [] }).length > 0);
  assert.ok(validateCancel({}).length > 0);
});

// -----------------------------------------------------------------------------------------------
// Row construction quirks, reproduced on purpose
// -----------------------------------------------------------------------------------------------

test('__waitForApproval parks a V1 row, and is ignored on V2', () => {
  const [item] = validV1();
  const parked = newRow(
    { ...item, context: { ...item.context, __waitForApproval: true } },
    { v2: false }
  );
  assert.equal(parked.status, STATUS.WaitingExecutionApproval);

  const v2 = newRow(
    {
      message: {},
      triggerAt: item.triggerAt,
      context: { __waitForApproval: true },
    },
    { v2: true }
  );
  assert.equal(v2.status, STATUS.Pending);
});

test('an absent period still produces processingData, because the guard tests !== null', () => {
  const row = newRow(
    {
      triggerAt: '2026-10-01T09:00:00.000Z',
      messageTopicName: 'x',
      context: { policyId: 'p' },
    },
    { v2: false }
  );
  assert.equal(row.period, null);
  assert.deepEqual(row.processingData, {
    beginAt: '2026-10-01T09:00:00.000Z',
    counter: 0,
  });
});

// -----------------------------------------------------------------------------------------------
// The dumb-service shape the console now uses
// -----------------------------------------------------------------------------------------------

test('a simple create needs a triggerAt and a message, and nothing else', () => {
  assert.deepEqual(
    validateCreateSimple({
      triggerAt: '2026-10-01T09:00:00.000Z',
      message: 'hello everyone',
    }),
    []
  );

  assert.ok(validateCreateSimple({ message: 'x' }).length > 0, 'no triggerAt');
  assert.ok(
    validateCreateSimple({ triggerAt: '2026-10-01T09:00:00.000Z' }).length > 0,
    'no message'
  );
  assert.ok(
    validateCreateSimple({
      triggerAt: '2026-10-01T09:00:00.000Z',
      message: '   ',
    }).length > 0,
    'blank message'
  );
  assert.ok(
    validateCreateSimple({
      triggerAt: '2026-10-01T09:00:00.000Z',
      message: 'x',
      period: 'every minute',
    }).length > 0,
    'period still has to be an ISO duration'
  );
});

test('a blank period makes a one-shot row, and no processingData with it', () => {
  const once = newSimpleRow({
    triggerAt: '2026-10-01T09:00:00.000Z',
    message: 'hello',
    period: '',
  });
  assert.equal(once.period, null);
  assert.equal(once.processingData, undefined);
  assert.equal(once.kind, 'simple');
  assert.equal(once.deliveries, 1);

  const repeating = newSimpleRow({
    triggerAt: '2026-10-01T09:00:00.000Z',
    message: 'hello',
    period: 'PT1M',
  });
  assert.equal(repeating.period, 'PT1M');
  assert.deepEqual(repeating.processingData, {
    beginAt: '2026-10-01T09:00:00.000Z',
    counter: 0,
  });
});

// -----------------------------------------------------------------------------------------------
// The V1 envelope every consumer of a fired schedule depends on
// -----------------------------------------------------------------------------------------------

test('V1 moves partnerId to scopePartnerId and drops it from the payload', () => {
  const message = buildV1Message({
    messageTopicName: 'policy-obligations',
    context: {
      command: 'processObligation',
      partnerId: 'partner-1',
      policyId: 'policy-1',
    },
  });

  assert.equal(message.topicName, 'policy-obligations');
  assert.equal(message.payload.authorizationData.scopePartnerId, 'partner-1');
  assert.equal(message.payload.authorizationData.partnerId, null);
  assert.equal(message.payload.partnerId, undefined);
  assert.equal(message.payload.command, 'processObligation');
  assert.deepEqual(message.payload.authorizationData.roles, ['alteosService']);
  assert.equal(message.payload.authorizationData.appKey, 'schedule-service');
  assert.equal(message.messageGroupId, undefined);
});

test('a .fifo topic sets messageGroupId to the policy, which is what preserves ordering', () => {
  const message = buildV1Message({
    messageTopicName: 'policy-obligations.fifo',
    context: { partnerId: 'partner-1', policyId: 'policy-1' },
  });
  assert.equal(message.messageGroupId, 'policy-1');
});

// -----------------------------------------------------------------------------------------------

let failed = 0;
for (const [name, fn] of tests) {
  try {
    await fn();
    console.log(`ok   ${name}`);
  } catch (error) {
    failed += 1;
    console.log(`FAIL ${name}\n     ${error.message}`);
  }
}
console.log(`\n${tests.length - failed}/${tests.length} passed`);
process.exit(failed === 0 ? 0 : 1);
