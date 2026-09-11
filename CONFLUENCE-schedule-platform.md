# Schedule platform — measured behaviour, load and coverage

> **For the session publishing this page**
>
> - Space: `pme`
> - Suggested parent: [RFC: Deprecate the schedule service in favor of AWS EventBridge Scheduler](https://versicorp.atlassian.net/wiki/spaces/pme/pages/5787025411) — this page is a companion to that RFC, not a replacement.
> - Suggested title: `Schedule platform — measured behaviour, load and coverage`
> - Labels: `schedule`, `eventbridge-scheduler`, `dpt-10337`
> - Every claim below is tagged **Measured**, **Computed** or **Open**. Please keep those tags when converting to Confluence markup — they are the point of the document. Do not promote an Open item to a conclusion.
> - Every diagram is ASCII and must stay inside a monospace code block. Mermaid was tried and
>   dropped on 11.09.2026: this Confluence has no Mermaid rendering, so a mermaid fence
>   publishes as plain code. Do not convert these back to Mermaid without checking that first.

Author: Bao (`bao-ext@alteos.com`). Reviewer requested: Benhur (`ben@alteos.com`).
Date of measurements: 10.09.2026. Epic: [DPT-10337](https://versicorp.atlassian.net/browse/DPT-10337).

---

## 1. Overview

### Why this page exists

The RFC decides *what* to build and prices it. This page records what was **measured** on a working
proof of concept, where the measurements disagree with assumptions in the RFC, and what is still
unknown. It exists because three assumptions that looked safe turned out to be wrong when tested,
and one of them changes a configuration decision nobody had been asked to make.

The headline: **the platform handles production peak load, but not for the reason the RFC gives.**
The RFC's load section reasons from a worst case of "every fire in a single hour". Production
measurement shows 98% of the peak day's fires land in a single **10-second** window — 360× more
concentrated. The conclusion survives; the reasoning and the required configuration do not.

### What was built

A working proof of concept in a personal AWS account, not the
Alteos development account. It carries the complete inbound surface of `services/schedule`, real
EventBridge Scheduler timers, a DynamoDB store, SQS standard and FIFO targets, a dead-letter queue,
a reconciler, and a console for driving it live.

It is a PoC, not a slice of the real implementation: the routes are ported faithfully but the
domain is reduced to "deliver this message at this time", because what is under question is the
platform, not the payload.

### Summary of findings

| Finding | Status | Section |
| --- | --- | --- |
| Peak load is 360× more concentrated than the RFC's worst case | Measured | 4 |
| The platform still handles it, clamped by the 1,000 TPS invocation quota | Computed | 4 |
| `FlexibleTimeWindow` removes four constraints with one field | Computed | 6 |
| SQS FIFO queue throughput is a new binding constraint | Open | 5 |
| The old service's slowness was smoothing load for its consumers | Computed | 5 |
| FIFO ordering across separate schedules is lost | Measured | 5 |
| `at()` in the past is accepted and fires in ~41 s | Measured | 3 |
| Reserved concurrency moves from precaution to requirement | Computed | 6 |
| Create is not idempotent; deliberately out of scope for the PoC | Decision | 2 |

---

## 2. Architecture

### The one rule

DynamoDB is the source of truth for intent and state. EventBridge Scheduler is **only** the timer.
Every inconsistency resolves in the direction of the DynamoDB row.

### Components

```
      caller
        │  lambda:Invoke, HTTP-shaped payload — no Function URL, no API Gateway.
        │  IAM decides who may call.
        ▼
  ┌─────────────────────────────────────────────────────┐
  │  poc-schedule — ONE Lambda, three entry shapes      │
  │                                                     │
  │   router      management routes                     │
  │               create / update / cancel / activate   │
  │   dispatcher  one fired occurrence                  │
  │   reconciler  every 60 s                            │
  └──────┬─────────────────┬──────────────────┬─────────┘
         │                 │                  │
         ▼                 ▼                  ▼
  ┌─────────────┐   ┌──────────────┐   ┌──────────────────┐
  │  DynamoDB   │   │ EventBridge  │   │  SQS             │
  │             │   │  Scheduler   │   │                  │
  │  SOURCE OF  │   │              │   │  standard  ──┐   │
  │   TRUTH     │   │  TIMER ONLY  │   │  FIFO      ──┼──▶ consumer
  │             │   │              │   │              │   │
  │  intent     │   │  holds no    │   │  dead-letter │   │
  │  + state    │   │  state, does │   │  queue    ◀──┘   │
  │             │   │  not read    │   │  (on_failure)    │
  │  GSIs:      │   │  DynamoDB    │   │                  │
  │  byPolicy   │   │              │   │                  │
  │  byStatus   │   │              │   │                  │
  └─────────────┘   └──────────────┘   └──────────────────┘
```

The arrow that does **not** exist is the important one: Scheduler never reads DynamoDB. It holds a
timer and an opaque payload, nothing else. Everything that knows what a schedule *means* lives in
the Lambda and the table.

### One artifact, three entry shapes

A single deployment serves all three roles, distinguished by the payload alone:

```
  event                                    guard              goes to
  ─────────────────────────────────────────────────────────────────────────────
  { requestContext.http, rawPath, … }      isHttpInvoke   →   router
  { scheduleId: "…" }                      isDispatch     →   dispatcher
  { source: "reconciler" }                 isReconciler   →   reconciler
  anything else                            —              →   logged, { ignored: true }
```

`source` is the literal string `"reconciler"`, not `"aws.events"`: Scheduler passes a target's
`Input` through untouched, unlike an EventBridge *rule*, so the tick arrives as the value Terraform
sets.

### What replaces what

| Concern | `services/schedule` | This platform |
| --- | --- | --- |
| Timing | `node-cron` every 60 s in every pod | one EventBridge Scheduler one-shot per pending occurrence |
| Work selection | `SELECT … WHERE status='pending' AND triggerAt < now LIMIT BATCH_SIZE` | none — each occurrence has its own timer |
| Mutual exclusion | Redis lock `locks:schedule:execute` | DynamoDB conditional write |
| Parallelism | `Promise.all` over one pod's batch | independent Lambda invocations |
| Ordering | sequential `for` over `.fifo` topics, per pod, per batch | SQS FIFO `MessageGroupId` only — see 5.3 |
| Failure handling | log and wait for the 30-minute sweeper | Lambda async retry, then `on_failure` dead-letter queue |
| Recurrence | next row inserted on the success path | next occurrence written in the same transaction as the current one's completion |
| Stuck work | sweeper flips `processing` back to `pending`, no ceiling | reconciler with a delivery ceiling, then terminal `failed` |

### The shape of the change

```
  services/schedule — one poller, many rows per pod

     every 60 s, in each of 2-8 pods
            │
            ▼
     ┌─────────────────┐   Redis lock 'locks:schedule:execute'
     │ acquire lock    │   redlock waits ~3-4.5 s, then throws
     └────────┬────────┘
              ▼
     ┌─────────────────────────────────────────┐
     │ SELECT pending, triggerAt < now         │
     │ ORDER BY triggerAt ASC  LIMIT BATCH_SIZE│
     │ UPDATE them → processing                │
     └────────┬────────────────────────────────┘
              ▼
     ┌─────────────────┐
     │ release lock    │  ◀── before any execution
     └────────┬────────┘
              ▼
     ┌──────────────────────┬──────────────────────────┐
     │ for (.fifo topics)   │ Promise.all(everything   │
     │   await execute      │   else)                  │
     │   — sequential, but  │   — concurrent            │
     │     only within THIS │                           │
     │     pod's batch      │                           │
     └──────────────────────┴──────────────────────────┘

     ceiling: pods x BATCH_SIZE per 60 s tick


  this platform — no poller, no lock, no batch

     occurrence 1 ──▶ timer 1 ──▶ invocation 1 ─┐
     occurrence 2 ──▶ timer 2 ──▶ invocation 2 ─┤
     occurrence 3 ──▶ timer 3 ──▶ invocation 3 ─┼──▶ SQS ──▶ consumer
        ...              ...          ...       │
     occurrence N ──▶ timer N ──▶ invocation N ─┘

     ceiling: Lambda concurrency (account-shared), and the 1,000 TPS
              invocation quota. Nothing per-tick.
```

Three things disappear rather than get bigger: the lock, the batch, and the pod count. What arrives
in their place is a quota and a concurrency limit — neither of which is a knob this service owns,
which is why section 6 exists.

### The old dispatch loop, precisely

Read from `services/schedule/src/schedule/processors/executeScheduledActions.ts`:

| Lines | What happens |
| --- | --- |
| 37–56 | select `pending` rows with `triggerAt < now`, `ORDER BY triggerAt ASC`, `LIMIT BATCH_SIZE` |
| 48–50 | acquire Redis lock `locks:schedule:execute`, TTL 10 000 ms |
| 59–77 | `Promise.all` — **booking only**, sets `status = processing` |
| 78 | **unlock, before any execution** |
| 111–114 | sequential `for` over topics ending `.fifo` |
| 116–121 | `Promise.all` over everything else |
| 305–307 | `isFifo` is decided by the topic name's `.fifo` suffix, not a flag |

Two properties of that loop matter for the comparison and are easy to misread:

**The lock waits rather than skips.** `CacheClient` builds redlock with `retryCount: 30`,
`retryDelay: 100 ms`, `retryJitter: 50 ms` — so a pod that finds the lock held retries for roughly
3–4.5 seconds before throwing. With 8 pods all firing cron on the same second, they queue through
the lock one at a time, and the `status='pending'` filter means each pod picks up a different batch.
Fleet throughput is therefore `pods × BATCH_SIZE` per tick, not `BATCH_SIZE` per tick.

**Execution happens outside the lock.** The unlock at line 78 is before the dispatch phase, so all
pods execute their batches concurrently. The sequential `for` over `.fifo` topics therefore orders
only *within one pod's batch* — never across the fleet. The old service's FIFO guarantee was already
weaker than the code reads at first glance.

### Fleet throughput of the old service

| Value | Source |
| --- | --- |
| `BATCH_SIZE` default | **10** — `services/schedule/src/common/config.ts:52-57`, env `ALTEOS_BATCH_SIZE` |
| Design README states | "up to 20 rows" |
| Pods | 2–7 normally, 6–7 on billing days (RFC), reported as 7–8 in practice |
| Effective ceiling used below | **160 rows / 60 s tick = 2.67 rows/s** (8 pods × 20) |

**Open:** the production values of `ALTEOS_BATCH_SIZE` and `ALTEOS_CRON_TIME` are unknown. The
code default and the README disagree. 160/tick is used throughout this page as the generous
reading; the pessimistic reading (8 × 10) halves every old-service figure below.

**Measured, minor:** the booking update is keyed on `id` alone with no status guard
(`executeScheduledActions.ts:62-65`). If a pod's critical section outlives the 10-second lock TTL,
two pods can book the same row and dispatch it twice. A narrow window, but it is the old service's
own duplicate-delivery path, and the new platform closes it with a conditional write.

### Where the platform's guarantee starts

The reconciler has exactly one scope: **reconcile DynamoDB against EventBridge Scheduler**. Every
inconsistency it can resolve is a disagreement between those two systems. Worth stating plainly,
because "the reconciler resolves every inconsistency" reads as a stronger promise than it is.

```
  +- caller's responsibility -----------+  +- the platform's guarantee ----------+
  |                                     |  |                                     |
  |  decide the intent                  |  |  the row in DynamoDB is the truth   |
  |  supply an idempotency key          |  |  the timer is armed, or repaired    |
  |  retry until the row exists         |  |  fire, retry, dead-letter           |
  |                                     |  |  the recurrence chain continues     |
  |  NOTHING here is recoverable by     |  |                                     |
  |  the platform: no record of the     |  |  every inconsistency from here on   |
  |  intent exists yet                  |  |  resolves toward the row            |
  +-------------------------------------+  +-------------------------------------+
                                           ^
                                      putRow succeeds
                                   the guarantee starts HERE
```

Three ways a create can fail, and who can repair each:

| Failed where | What exists afterwards | Who can repair it |
| --- | --- | --- |
| the invoke never reached the function | **nothing** — no row, no timer | **only the caller**, by retrying. The platform holds no record of the intent, so no amount of reconciliation recovers it. |
| the row was written, `CreateSchedule` threw | row `pending` with no timer | **the platform.** The reconciler arms it on the next tick, from the row's own `triggerAt`. |
| both succeeded, the response was lost | row `pending` **with** its timer — the schedule will fire | nobody needs to. But the caller believes it failed. |

The middle row is the one the reconciler exists for, and the reason `putRow` runs *before*
`CreateSchedule`: a create that fails halfway leaves a self-healing state. Reversing the two would
leave a timer with no row, which fires into nothing.

The third row is the dangerous one. The work is already scheduled, the caller believes otherwise,
and a retry produces a *second* schedule for one intention.

### Idempotency — not implemented, deliberately

**A retry is only safe when the caller supplies the id.** The timer's name is derived from the row
id, so with a caller-supplied id a retry is naturally idempotent — the design already says so:

> **A retried create is idempotent.** The CRUD handler writes the row, then creates the timer. A
> `ConflictException` means the first attempt actually succeeded. No duplicate timer, either way.
> — `serverless/schedule/docs/schedule-store.md`

**This PoC does not do that.** `newSimpleRow` mints a server-side `randomUUID()`, and `putRow` is an
unconditional `PutCommand`. So a caller retry produces a new id, a new row and a new timer — a
duplicate. The irony is that `createSchedule` already catches `ConflictException`; it simply never
fires, because nothing ever presents the same id twice.

**Decision: out of scope for the PoC.** What the PoC exists to answer is whether the platform's own
state machine holds — store as the source of truth, timer as only a timer, and what retry,
dead-lettering and reconciliation do. Delivery of a create request is a different question, it is
answered the same way for any service behind an SDK call, and building it here would have added a
surface without testing anything that was in doubt.

Two things that must not be lost in that decision, because they are the reason it is a *decision*
and not an omission:

**The responsibility is split, not delegated.** The caller owns the retry. The platform owes the
caller a key to retry *with*. A platform that mints ids server-side makes a correct caller produce
duplicates, so "idempotency is the caller's concern" is only half true — and the half the platform
owes is specified in `schedule-store.md` and must be built in the real implementation.

**A caller-supplied id needs a conditional write.** With the id coming from outside, an
unconditional `PutCommand` lets a late retry overwrite a row that has since moved to `processing`,
resetting it to `pending` and firing the work twice. The write needs
`ConditionExpression: attribute_not_exists(id)`, with `ConditionalCheckFailedException` treated as
success — the same shape as the `ConflictException` handling beside it. `schedule-store.md` says
"writes the row, then creates the timer" and does not pin this down; see section 8.

**Not a regression.** `services/schedule` has the same gap — `createScheduledActions` generates the
id server-side through Sequelize. This is unsolved rather than newly broken, which is a reason to
specify it now and not a reason to let it pass.

### The fire path

```
  Scheduler        poc-schedule        DynamoDB          SQS        consumer
      |                 |                  |              |             |
      |  1 invoke (async), Input is only {scheduleId}     |             |
      |---------------->|                  |              |             |
      |                 |  2 GetItem       |              |             |
      |                 |----------------->|              |             |
      |                 |  3 UpdateItem, conditional      |             |
      |                 |    claim to processing          |             |
      |                 |----------------->|              |             |
      |                 |    a lost claim STOPS HERE: logged            |
      |                 |    notClaimable, nothing published            |
      |                 |  4 SendMessage   |              |             |
      |                 |-------------------------------->|             |
      |                 |  5 TransactWriteItems           |             |
      |                 |    current -> executed AND      |             |
      |                 |    next occurrence -> pending   |             |
      |                 |----------------->|              |             |
      |  6 CreateSchedule for the next occurrence         |             |
      |<----------------|                  |              |             |
      |                 |                  |              |  7 deliver  |
      |                 |                  |              |------------>|
```

Two details in that order matter more than they look.

**The timer carries only the id.** Everything else is read from the row at fire time. That is why a
message can be edited without touching the timer, and why the timer can be rebuilt from the row
alone.

**Completion and the next occurrence are one transaction** (step 5). The old service wrote
`executed` first and inserted the next row afterwards, inside a `try` whose `catch` only logs — so a
failure there ended a monthly chain silently and nothing looked for the schedule that stopped
existing. Reproduced live on this PoC, then closed with `TransactWriteItems`.

The `CreateSchedule` at step 6 is deliberately *outside* the transaction. If it throws, the next
occurrence is already `pending` with no timer, which is exactly the drift the reconciler repairs —
so the chain cannot be lost by a failure there.

### The failure path, and the two budgets

The single easiest thing to get wrong in this design. There are two independent budgets and they
count different things.

```
  attempts    how many Lambda invocations this ONE firing has used
              spent by Lambda's own async retry
  deliveries  how many firings this occurrence has used
              spent by the reconciler re-arming the row
```

```
  a firing throws — the publish, or the chain write
        |
        +- attempts += 1
        |
        +- attempts >= invocationsPerFiring ?
             |
             +- NO --> row stays processing, re-throw.
             |         Lambda retries THIS SAME firing ---+
             |                                            |
             |    <---------------------------------------+
             |
             +- YES -> Lambda's async retries are exhausted:
                       on_failure writes ONE dead-letter record
                       |
                       +- deliveries >= maxDeliveryAttempts ?
                            |
                            +- NO --> row stays processing,
                            |         this firing is spent
                            |         |
                            |         +- reconciler, after the processing TTL:
                            |            deliveries += 1, attempts = 0,
                            |            row -> pending, new timer --> fires again
                            |
                            +- YES -> row -> failed + failedAt
                                      TERMINAL: nothing re-fires it,
                                      no timer is created
```

Three consequences worth stating plainly:

**A throwing dispatcher is still a *delivered* fire.** Scheduler invokes asynchronously, so it has
already done its job; only `aws_lambda_function_event_invoke_config` with `on_failure` catches the
failure, and `sqs:SendMessage` for the dead-letter queue must be on the **function's** role, not the
scheduler's.

**One dead-letter record per exhausted firing, not per failed attempt** — and not only at the
terminal state. A row with two deliveries allowed produces two dead-letter records before it parks.

**Re-firing lives in the reconciler alone.** A second component deciding to fire is a second
component that can decide wrong.

Contrast with what is being replaced: the old service logged the error, left the row in
`processing`, and the 30-minute sweeper put it back — with no attempt ceiling, no backoff and no
quarantine. An action that can never succeed retried for ever and nothing reported it.

### The reconciler

```
  tick — every 60 s
        |
        +- read: pending rows, processing rows,
        |        live timer names in the group
        |
        +- pending row with no timer ?
        |     +- YES -> CreateSchedule from the row's own triggerAt,
        |               VERBATIM — not from now
        |
        +- timer with no pending row ?
        |     +- YES -> DeleteSchedule — orphan.
        |               Only names this platform minted;
        |               Terraform owns the reconciler's own
        |
        +- processing row older than the TTL ?
              +- YES -> deliveries >= max ?
                          +- YES -> row -> failed, gave up
                          +- NO --> row -> pending, deliveries += 1,
                                    attempts = 0, CreateSchedule
```

The reconciler has **no** path that compares `triggerAt` to now, and **no** path that calls the
dispatcher. It cannot make a row fire — it only rebuilds the clock. That is what the isolating run
in 3.4 confirms from the outside.

"Verbatim" in the repair branch is the load-bearing word. A row whose `triggerAt` has already passed
gets a timer with a past `at()`, which Scheduler accepts and fires in about 41 seconds (3.3). If
past `at()` were rejected, the reconciler would have to invent a time — which means silently
rewriting the caller's intent.

### Status machine

```
   create                                 create with waitForApproval
      |                                                |
      v                                                v
  +---------+                        +--------------------------+
  | pending |<-- update / retime     | waitingExecutionApproval |<-- update / retime
  +----+----+                        +------------+-------------+
       |                                          |
       |<----------------- activate --------------+
       |
       +- cancel --> cancelled --> [end]
       |
       +- claim (conditional write) --> +------------+
                                        | processing |<-- claim again, the retry
                                        +-----+------+    of the same firing
                                              |
                                              +- published + chain step committed
                                              |     --> executed --> [end]
                                              |
                                              +- attempt failed, budget left
                                              |     --> stays processing
                                              |
                                              +- both budgets spent --> failed --> [end]
                                              |
                                              +- reconciler unstuck, after the TTL
                                              |     --> pending
                                              |
                                              +- reconciler gave up --> failed --> [end]

   activate writes pending from ANY status with no status guard — so executed,
   cancelled and failed rows can all be resurrected. Ported deliberately: the
   old service updated by id IN (...) with no condition. Almost certainly not
   intended there either.
```

Two honest notes about this machine:

`preExecuted` is declared in the status enum because the service being replaced has it, but
**nothing in this PoC ever writes it**. It is carried for surface compatibility, not behaviour.

`waitingExecutionApproval` is only ever set at creation, by the `waitForApproval` flag on the V1/V2
routes. It leaves that state only through `activate`, or through `update` keeping it in place — a
parked row is deliberately not given a timer, which is what `activate` is for.

---

## 3. Measured platform behaviour

All figures below were measured on 10.09.2026 against the live PoC stack. Where a number is a
range, it is bounded by the observation interval, not by variance.

### 3.1 Dispatcher duration

**Measured: 43–71 ms** per fire, for a dispatcher that already does the real work — read the row,
conditional-claim it, publish to SQS, write the chain step transactionally, create the next timer.

The RFC's cost model assumes ~300 ms. That is conservative by roughly 5×, which is the right
direction for cost. It matters far more than cost, though, because concurrency is
`arrival rate × duration` — see section 4.

### 3.2 Delivery latency

| Case | Latency | Samples |
| --- | --- | --- |
| `at()` at a future time | 4–32 s after the nominal time | 3 |
| `at()` already in the past, at create | 41.2 / 41.2 / 41.9 s after **creation** | 3 |
| `at()` already in the past, via `UpdateSchedule` | 41.9 s after the call | 1 |

### 3.3 A past `at()` is accepted, stored verbatim, and fires

This was tested because an earlier version of the PoC assumed the opposite and clamped a past
target forward to `now + 10 s`.

**Measured:** Scheduler accepts `at()` in the past, does **not** clamp it — `GetSchedule` reads back
`at(2026-09-10T06:36:19)` for a time five minutes gone — and invokes the target about 41 seconds
later, the same latency as any other fire.

This matters beyond the API surface: it is what makes the reconciler's repair correct. The
reconciler recreates a missing timer from the row's stored `triggerAt`, **verbatim** — not from
"now". If a past `at()` were rejected, the reconciler would have to invent a new time, which means
silently rewriting the caller's intent.

### 3.4 The fire is Scheduler's own, not a reconciler repair

The obvious objection to 3.3 is that the reconciler noticed an overdue row and re-created its timer.
It did not.

**Measured, isolating run:** a schedule created straight through the SDK — no DynamoDB row anywhere,
so nothing for the reconciler to sweep, and a name that does not match the reconciler's
orphan-deletion pattern — was updated to a time five minutes past and invoked the function 41.9 s
later. The reconciler ticks either side of it reported `pending:0, processing:0, repaired:0,
orphansDeleted:0`.

Supporting code reading: the reconciler repairs missing timers, deletes orphans and unsticks
`processing` rows. It has no path that compares `triggerAt` to now and no path that calls the
dispatcher. It cannot make a row fire — it only rebuilds the clock.

### 3.5 There is no periodic sweep inside Scheduler

**Measured:** three probes, all aimed 10 minutes into the past, created 25 seconds apart, fired
41.31 / 41.22 / 41.90 s after **their own creation** — spaced like their creations, not clustered on
a wall-clock boundary, and with a spread of 0.7 s across the three.

A periodic sweeper would produce delays that vary with where creation falls in the cycle, and would
cluster the three fires together. Neither happened. The behaviour is consistent with Scheduler
computing due-now at creation time.

Consequence for the design: the reconciler's repair of an overdue occurrence carries no hidden floor
beyond its own cadence. Recovery time is `reconciler cadence + ~41 s`, with nothing else added.

**Open:** three samples, one region, one session. AWS documents none of this. Enough to say it
works; not enough to promise a latency.

### 3.6 Retime works in both directions, including into the past

The service being replaced has **no update endpoint at all**. Nothing forbade one — there was simply
never one to call, which is why `workflow` moves a termination date by cancelling and creating. The
design has `PATCH /v1/schedules/:id` ("retime or repayload").

**Measured:**

| Updated to | Accepted by Scheduler | Fired | Delay |
| --- | --- | --- | --- |
| later (`+2 m` → `+6 m`) | yes, verbatim | at the new time, **not** at the old one | 23.8–39.7 s late |
| earlier, still future (`+12 m` → `+2 m`) | yes, verbatim | at the new time | 10.5–21.3 s late |
| already gone (`+12 m` → `−5 m`) | yes, verbatim | straight away | 35.6–46.4 s after the call |

The "not at the old one" is an observation, not an inference: the row was polled at 15-second
intervals straight through the old timestamp and stayed `pending`. Its timer list held exactly one
name throughout, so `UpdateSchedule` replaces rather than adds — which cancel-and-recreate cannot
offer, being two writes to two systems with a window in between.

Also measured: a message-only edit leaves `triggerAt` and the recurrence anchor untouched, and a
time-only edit re-anchors the chain and preserves the message. Both matter — always applying the
full patch would reset a recurring row's counter on a text correction, silently breaking its
rhythm one fire later.

### 3.7 Concurrency: bursts of identical timers

Rows created with an identical `triggerAt`, then left alone (no polling, so every invocation in the
log window is the platform's own).

| Rows on one timestamp | Delivery spread | Distinct containers | Max simultaneous invocations | Cold starts |
| --- | --- | --- | --- | --- |
| 5 | **43.0 s** | 1 | 1 | 0 |
| 20 | **38.1 s** | 2 | 2 | 0 |

The N=20 run in full. Every one of these timers was set to the same instant, and the container
column is the CloudWatch log stream — a container serves one invocation at a time, so two streams
active at once is direct evidence of concurrency.

```
  20 timers, all set to   at(2026-09-10T07:52:18.745)

   fired at         + from nominal   row      container   note
   ---------------------------------------------------------------------------------
   07:52:27.083          +8.3 s      10/20        A
   07:52:28.057          +9.3 s      14/20        A
   07:52:29.075         +10.3 s      11/20        A
   07:52:30.102         +11.4 s       5/20        A
   07:52:32.081         +13.3 s      13/20        B       container B first appears
                                                          (238 ms, its slowest)
   07:52:34.065         +15.3 s      15/20        A
   07:52:35.046         +16.3 s       7/20        A
   07:52:36.085         +17.3 s       8/20        A
   07:52:40.086         +21.3 s       3/20        A   -+
   07:52:40.087         +21.3 s      16/20        B   -+  1 ms apart  -> OVERLAP
   07:52:42.068         +23.3 s      17/20        B
   07:52:43.099         +24.4 s      12/20        B
   07:52:46.055         +27.3 s      18/20        B   -+
   07:52:46.069         +27.3 s       9/20        A   -+  14 ms apart -> OVERLAP
   07:52:47.064         +28.3 s       6/20        A
   07:52:48.055         +29.3 s      20/20        A
   07:52:50.079         +31.3 s       2/20        A
   07:52:53.094         +34.3 s       4/20        A
   07:52:57.066         +38.3 s      19/20        A
   07:53:05.084         +46.3 s       1/20        A

   spread 38.1 s  ·  each invocation 39-71 ms  ·  2 containers
   max simultaneous 2  ·  cold starts 0
```

Read the `row` column downwards and it is the finding of 3.8 in raw form: the creation order
1..20 comes back as 10, 14, 11, 5, 13, 15, 7, 8, 3, 16, 17, 12, 18, 9, 6, 20, 2, 4, 19, 1.

Two results, both surprising:

**Scheduler does not deliver simultaneous timers simultaneously.** Five timers sharing one `at()`
fired 15 s, 4 s, 22 s and 2 s apart. At N=5 there was no concurrency at all to observe — the
invocations never overlapped, so Lambda reused a single warm container.

**The spread does not grow with N.** Four times the rows, the same ~40-second spread. So that window
is Scheduler's delivery jitter, not a serial rate limit. Concurrency appeared at N=20 only because
two pairs of fires landed 1 ms and 14 ms apart, and at ~50 ms of work each that is enough to force a
second container.

**Important caveat:** both runs recorded **zero cold starts**, because the containers were warm from
earlier tests. A real billing night at 00:00 follows 29 quiet days, so every container is cold.
These numbers therefore do **not** represent the burst edge. See 7.2.

### 3.8 Publish order is not preserved

**Measured**, N=20 on one timestamp, publish order taken from dispatcher log timestamps:

```
creation order:  1  2  3  4  5  6  7  8  9 10 11 12 13 14 15 16 17 18 19 20
publish order:  10 14 11  5 13 15  7  8  3 16 17 12 18  9  6 20  2  4 19  1
```

Fully scrambled. The mechanism is not parallel dispatchers racing — at N=5 they ran strictly
serially and the order was still arbitrary. **Scheduler delivers identical timestamps in arbitrary
order.** See 5.3 for what that costs.

---

## 4. Measured load and cost model

### 4.1 The RFC's figures

From the RFC's *Measured load & cost model*, 30 days to 26.07.2026:

| Metric | Value |
| --- | --- |
| Total fires / 30 days | 63,652 |
| Average / day | ~2,122 |
| Median / day | ~1,381 (≈1/min) |
| Peak day | 22,325 on 01.07.2026 (monthly billing run; ≈15.5/min averaged over the day) |
| Planning basis | 5× measured → ~318k fires/month; 5× billing peak ≈112k fires/day |

The section's load argument:

> even if every one of them landed in a single hour that is ~31 fires/s, far below the 1,000 TPS
> invocation quota (and Scheduler throttling delays rather than drops)

### 4.2 Production shape, measured

Two aggregate exports from `scheduledActions` (counts only — no policy or customer identifiers).

**Median day, 10.09.2026 — 1,407 fires:**

| Hour | Topic | Count |
| --- | --- | --- |
| 00:00 | `policy-jobs.fifo` | 1,024 |
| 00:00 | `accounting-jobs` | 275 |
| 00:00 | `workflow-jobs` | 44 |
| 12 other hours | all topics | 1–5 each |

95% of the day in one hour.

**Peak day, 01.07.2026 — 22,322 fires, bucketed by 10 seconds:**

| 10-second bucket | Topic | Count |
| --- | --- | --- |
| `2026-07-01T00:00:00` | `policy-jobs.fifo` | **19,658** |
| `2026-07-01T00:00:00` | `accounting-jobs` | **2,189** |
| `2026-07-01T00:00:00` | `workflow-jobs` | 24 |
| `2026-06-30T23:59:50` | `workflow-jobs` | 429 |
| 14 other buckets, whole day | all topics | 22 total |

**21,871 fires — 97.98% of the peak day — share a single 10-second bucket.** Including the adjacent
bucket, 99.90% land within 20 seconds.

```
  peak day 01.07.2026 — fires per 10-second bucket, whole day

  00:00:00  ################################################  21,871   97.98%
  23:59:50  #                                                    429    1.92%
  14 other buckets, spread across the other 24 hours                22    0.10%

  the RFC's worst case, next to what was measured

    "every fire in a single hour"    22,322 over 3,600 s  =      6.2 /s
    measured                         21,871 over     10 s  =  2,187.1 /s
                                                              ^^^^^^^
                                                              353x the rate,
                                                              360x the concentration
```

This is not a long tail with a spike. It is a single discharge with a rounding error attached. Any
argument about this platform's load that starts from a daily or hourly average is answering a
question production does not ask.

Both exports cross-check against the RFC: 1,407 against a stated median of ~1,381 (within 2%), and
22,322 against a stated peak of 22,325 (within 3). The datasets agree, so neither is an artefact.

### 4.3 Recomputing the load argument

| | RFC | Measured |
| --- | --- | --- |
| Concentration window | 1 hour | **≤10 seconds** |
| Offered rate at 5× (109,355 fires) | 31/s | **10,936/s** |
| Versus the 1,000 TPS invocation quota | "far below" | **11× over** |

**Computed**, clamped by the quota to ~1,000 fires/s:

| | 1× (measured peak day) | 5× (RFC planning basis) |
| --- | --- | --- |
| Fires on one timestamp | 21,871 | 109,355 |
| Time to deliver, at the quota | **~22 s** | **~109 s** |
| Concurrency at 50 ms (measured duration) | 50 | 50 |
| Concurrency at 300 ms (RFC's assumption) | **300** | **300** |

Because the quota clamps the arrival rate, **concurrency does not scale with load** — only the
duration of the burst does. 1× and 5× both sit at 50–300 concurrent; the difference is 22 seconds
versus 109 seconds.

**The platform handles peak load.** But not because the rate is far below the quota — it is above
it, and what saves the design is that Scheduler throttles by delaying rather than dropping, exactly
as the RFC's parenthesis says. The parenthesis turns out to be the load-bearing part of the
argument, and the main clause is wrong.

At 300 ms of work per fire, 300 concurrent is **30% of the account's entire default Lambda
concurrency**, held for roughly 110 seconds, on every billing day. That is the number that turns
reserved concurrency from a precaution into a requirement.

### 4.4 Drain time, old versus new

**Computed**, old service at 160 rows per 60-second tick:

| Load | `services/schedule` | This platform |
| --- | --- | --- |
| Median day, 1,343 at midnight | **8.4 minutes** | ~22 s |
| Measured peak day, 21,871 | **2.3 hours** | ~22 s, or ~66 s if FIFO clamps (5.2) |
| 5× planning, 109,355 | **11.4 hours** | ~109 s, or ~5.5 minutes if FIFO clamps |

Production currently takes **2.3 hours** to fire the work that was due at 00:00 on a billing day.
This figure is not in the RFC and is the strongest single argument for the replacement.

### 4.5 Cost model

The RFC's cost model is reproduced here unchanged, because nothing measured contradicts it. The
measured dispatcher duration of 43–71 ms against the assumed ~300 ms means the Lambda line is
conservative by roughly 5×.

| Item | Estimate at 5× load |
| --- | --- |
| EventBridge Scheduler (318k invocations) | $0 inside the 14M/month free tier ($0.32 without; dormant schedules are free) |
| Lambda (~700k invocations; 256 MB, ~300 ms) | ~$1.0 |
| DynamoDB (~640k writes, ~950k reads, a few GB) | ~$1.5 |
| **Total** | **≈$3/month** (well under $1 at today's load) |

Removed: 2–7 Kubernetes pods at ~$25–120/month depending on whether pods are costed at requests or
limits; the `schedule-db` Postgres database dropped; `schedule-cache` Redis lock traffic removed.

**Open:** the pod figure is the RFC's own soft number and still needs node pricing from infra.

---

## 5. Constraints the load section does not cover

### 5.1 The invocation quota is reached, not avoided

Covered in 4.3. Restated here because it belongs in the RFC's own open questions: at the planning
basis, the offered rate is 11× the 1,000 TPS invocation quota, and the design depends on
throttle-as-queue behaviour. That is acceptable for a nightly billing batch and should be stated
deliberately rather than arrived at by accident.

Interaction to design for: throttled **asynchronous** invocations retry. With a tight retry budget,
a 110-second throttle window can push healthy jobs into the dead-letter queue while nothing is
actually broken. The correct lever is a generous `maximum_event_age_in_seconds`, not more retry
attempts.

### 5.2 SQS FIFO queue throughput — the tightest constraint, and unverified

19,658 messages in one 10-second bucket all target **`policy-jobs.fifo`**.

A FIFO queue has its own throughput ceiling, and it does not auto-scale. Without high-throughput
mode and without batching, the documented ceiling is in the low hundreds of messages per second.

**Computed** at 300 messages/s:

| Load | Messages to `policy-jobs.fifo` | Time to publish |
| --- | --- | --- |
| Measured peak day | 19,658 | **66 s** |
| 5× planning | 98,290 | **5.5 minutes** |

The problem is not the duration. The problem is the mismatch: Scheduler delivers at ~1,000/s into a
queue that accepts far less, so `SendMessage` is throttled, the dispatcher throws, Lambda retries,
and the retry budget decides whether the job survives. A limit at the queue layer becomes
dead-lettering at the platform layer.

The old service never met this constraint because it published at 2.67/s — roughly two orders of
magnitude below the ceiling. **Its slowness was hiding a limit the new platform meets immediately.**

**Open, and the highest-priority item on this page:**

1. Does `policy-jobs.fifo` have high-throughput mode enabled? Cannot be checked from the PoC
   account.
2. How many **distinct `policyId` values** are in those 19,658? This decides both the ordering
   question below *and* the throughput question, because FIFO throughput is also limited per message
   group. Few policies with many actions each is the worst case for both.

```sql
SELECT ("context"->>'policyId') AS policy_id, COUNT(*) AS actions
FROM "scheduledActions"
WHERE "messageTopicName" = 'policy-jobs.fifo'
  AND "triggerAt" >= TIMESTAMP '2026-07-01 00:00:00'
  AND "triggerAt" <  TIMESTAMP '2026-07-01 00:00:10'
GROUP BY 1 ORDER BY actions DESC LIMIT 20;
```

### 5.3 FIFO ordering across separate schedules is lost

Two different things get called "ordering" and only one of them is at risk.

**Safe, by construction: ordering within one recurring chain.** The timer for occurrence N+1 is
created only after occurrence N has fired. N+1 cannot precede N, because until N fires its timer
does not exist. The old service had the same property, through inserting the next row on the success
path.

**Lost: ordering between different schedules in the same message group.** This is what the old
service's sequential `for` handled, and 3.8 measures it as fully scrambled. `MessageGroupId`
preserves the order in which messages are *consumed* relative to the order they *arrived*; it has
no influence on arrival order.

The regression is smaller than it first appears, because the old guarantee was already partial —
execution happened outside the lock, so ordering held only within one pod's batch, never across 8
pods. But it is a regression.

**Evidence that the domain has at least one order-sensitive pair:** in `executeActionV1`, when
`context.command` is `processObligation`, the code fetches the policy's `concludePolicy` action and
declines to create the next occurrence if it would fall after that date
(`executeScheduledActions.ts:209-230`). That code governs row *creation*, not publish order, so it
does **not** prove a regression — but it does establish that `processObligation` must not happen
after `concludePolicy`, which makes the question worth answering rather than assuming.

**Open:** whether any consumer depends on ordering between distinct schedules of one policy. The
`policyId` query in 5.2 is the first step; interpreting the result is a question for the BI
Champions group (Ievgen) and the owning teams, not something to infer from code.

### 5.4 The consumers lose a load smoother

This is the largest unexamined consequence on this page.

Today, 19,658 messages reach `policy-jobs.fifo` **spread across 2.3 hours**, because the old service
can only drain 160 rows per minute. The `policy` service has been consuming at that pace for years.

This platform delivers the same messages in **22 to 66 seconds** — a compression of roughly 125× to
375×.

The platform does not simply become faster in a harmless way. It **moves the peak downstream**, onto
services that have never seen it and have not been asked whether they can take it. The old service's
slowness was acting as an unintentional rate limiter for everything behind it.

Two honest options, and the choice is a decision rather than a configuration:

1. Confirm with the consuming teams that a 125× compressed burst is acceptable.
2. Re-introduce the smoothing deliberately, with a number — which is what section 6 is about.

**Open:** consumer capacity under a compressed burst. Needs the owning teams. Should be settled
before any per-caller cutover, not after.

---

## 6. What to configure, and the one field that does most of the work

### 6.1 What auto-scales and what does not

| Layer | Auto-scales? | Action needed |
| --- | --- | --- |
| Scheduler — creating timers | yes; 5,000 TPS `CreateSchedule` quota | none (21,871 timers = 4.4 s) |
| Scheduler — storing timers | yes; 10M schedule quota | none |
| Scheduler — delivering timers | **no**, fixed 1,000 TPS quota | accept the delay, or request an increase |
| Lambda concurrency | yes, **within a ramp rate** | set reserved concurrency |
| DynamoDB on-demand | yes, **warms from previous peak** | verify; see 7.2 |
| SQS standard | yes, effectively unbounded | none |
| **SQS FIFO** | **no**, fixed ceiling | verify high-throughput mode (5.2) |
| Consumers | out of scope here | decide (5.4) |

### 6.2 `FlexibleTimeWindow`

The PoC uses `FlexibleTimeWindow` with `Mode: OFF` — fire at the exact second. Scheduler also
supports spreading a fire across a window:

```hcl
flexible_time_window {
  mode                      = "FLEXIBLE"
  maximum_window_in_minutes = 15
}
```

**Computed**, 21,871 fires with a 15-minute window:

| | `Mode: OFF` | `FLEXIBLE`, 15 minutes |
| --- | --- | --- |
| Offered rate | ~10,936/s, clamped to 1,000/s | **24/s** |
| Concurrency at 300 ms | 300 | **7** |
| Exceeds the 1,000 TPS invocation quota | yes | no |
| Exceeds a default FIFO queue ceiling | yes | no |
| DynamoDB write spike | yes | no |
| Compressed burst onto consumers | yes | no |

**One Terraform field removes all four constraints.** No rate limiter to write, no extra SQS buffer,
no reserved concurrency acting as the primary brake.

Three things to know before choosing it:

- Jobs fire **within** the window, not at an exact second. For a midnight billing batch that is
  almost certainly fine, but it is a business decision, not a technical one.
- Scheduler distributes randomly inside the window, which makes the **ordering** question in 5.3
  *worse*. If ordering matters for a given topic, these two goals conflict.
- It is set **per schedule**, so it can be enabled for the billing batch alone and left `OFF` for
  schedules that must hit an exact time.

### 6.3 Recommended configuration

| Setting | Recommendation | Why |
| --- | --- | --- |
| `FlexibleTimeWindow` | `FLEXIBLE`, 15 minutes, for the midnight batch topics only | 6.2 |
| Reserved concurrency | set it; size it from the chosen window | 4.3 — 30% of the account limit otherwise |
| `maximum_event_age_in_seconds` | generous | 5.1 — a throttle window must not become dead-letter traffic |
| Retry attempts | do **not** tighten to compensate for throttling | 5.1 |
| DynamoDB capacity mode | verify on-demand behaviour against a monthly cold peak | 7.2 |

---

## 7. Coverage — what this PoC proves, and what it does not

### 7.1 Proven on the live stack

| Area | Evidence |
| --- | --- |
| Complete inbound surface | all 7 routes of `services/schedule` ported, plus 11 behaviours not documented anywhere, each identified with a file and line |
| Dead-letter path | `failedAt` on the row and the dead-letter record present within 1 second of the final failed attempt |
| Reconciler — missing timer | timer deleted underneath a live `pending` row; repaired on the next tick from the row's own `triggerAt` |
| Reconciler — orphan timer | row deleted underneath a live timer; timer removed on the next tick |
| Anti-drift recurrence | `beginAt + period × (counter + 1)` held over 169 consecutive occurrences with no accumulated drift |
| Silent chain break | reproduced live — `executed` written before the next occurrence, with one `catch` that only logs — then closed with `TransactWriteItems` |
| Retry budgets | Lambda async retries (invocations per firing) and platform deliveries (firings) separated; conflating them caused two distinct bugs |
| Retime / repayload | both directions including into the past; message-only and time-only edits verified independent (3.6) |
| Past `at()` semantics | with the reconciler excluded as the cause by an isolating run (3.4) |
| Concurrency behaviour | at N=5 and N=20 (3.7) |

### 7.2 Covered by the design but unproven at scale

| Item | Why it is not proven |
| --- | --- |
| Whether the ~40 s delivery spread holds at N in the thousands | measured only at N=5 and N=20 |
| Cold-start ramp at the burst edge | both burst runs recorded **zero** cold starts because containers were warm; a real 00:00 burst follows 29 quiet days and is entirely cold. Duration inflates concurrency, which forces more containers, which are also cold — a positive feedback loop at exactly the wrong moment. Not measured; magnitude unknown. |
| DynamoDB on-demand under a monthly cold peak | on-demand scales from the *previous* peak. A table doing ~0.4 writes/s for 29 days and then 3,000–4,000 writes/s for 22 seconds will throttle while it stretches. A 15-minute flexible window reduces this to roughly 100 writes/s and largely removes the concern. |
| Consumer idempotency | the platform is at-least-once, as the old service was. A fire that published and then failed publishes again on retry. Not verified against any real consumer. |

### 7.3 Not covered

| Item | Note |
| --- | --- |
| Idempotent create | server-minted ids and an unconditional `PutCommand`, so a caller retry duplicates. Deliberately out of scope — see section 2. The real implementation must accept a caller-supplied id and write it conditionally. |
| Kafka leg | `target.transport` accepts `kafka` in the schema; the publish path is not built, no caller consumes over Kafka, and the topic contract is unsettled |
| Mitigation for lost FIFO ordering | the loss is measured (3.8, 5.3); nothing is built to address it, pending the `policyId` data |
| SQS FIFO throughput headroom | 5.2 — cannot be checked from the PoC account |
| Reserved concurrency and retry-age tuning | 6.3 — recommended, not implemented |
| Scheduler execution-role trust policy | the two-statement `aws:SourceArn` shape is **not** proven. Three hypotheses were tested and each disproved. Needs a dedicated trial harness; must not be written up as settled. |
| `ALTEOS_CRON_TIME`, `ALTEOS_BATCH_SIZE` | production values unknown; the code default and the design README disagree on batch size |
| Whether every old `period` is expressible as `rate()`/`cron()` | only relevant if the design moves to "EventBridge owns recurrence"; this PoC computes the next occurrence itself |

---

## 8. Requested changes to the RFC

None of these change the RFC's decision. All of them change its reasoning or its open questions.

1. **Replace the "single hour → ~31 fires/s" worst case** with the measured shape: 97.98% of the
   peak day in a single 10-second bucket. Keep the conclusion; the concentration is 360× higher and
   the quota is reached rather than avoided (4.3).
2. **Add concurrency as a distinct concern from TPS.** `concurrency = arrival rate × duration`. At
   the quota clamp this is 50–300, independent of 1× versus 5×. Reserved concurrency is a
   configuration decision that will otherwise be skipped (4.3, 6.3).
3. **Add a design constraint: the dispatcher must stay thin.** Measured at 43–71 ms. At 3 seconds
   the same load needs ~3,000 concurrent. Publish and exit; heavy work belongs behind SQS.
4. **Add SQS FIFO throughput to open questions** (5.2), with the `policyId` query as the first step.
5. **Add consumer burst capacity to open questions** (5.4). This gates per-caller cutover and is
   currently nobody's item.
6. **Add `FlexibleTimeWindow` as the primary load-shaping lever** (6.2), including its conflict with
   ordering.
7. **Add the 2.3-hour / 11.4-hour old-service drain times** (4.4) to the comparison. They are the
   strongest argument for the replacement and are currently absent.
8. **Pin down the conditional write behind the idempotent create.** `schedule-store.md` says the
   handler "writes the row, then creates the timer" without saying the row write is conditional.
   With a caller-supplied id it must be — `attribute_not_exists(id)`, with
   `ConditionalCheckFailedException` treated as success — or a late retry resets a `processing` row
   to `pending` and the work fires twice.

---

## 9. Open questions, with owners

| # | Question | Needed from | Blocks |
| --- | --- | --- | --- |
| 1 | Does `policy-jobs.fifo` have high-throughput mode enabled? | infra / Ale | sizing, and whether batching is required |
| 2 | How many distinct `policyId` values in the 19,658-fire bucket? | BI Champions (Ievgen) | both ordering (5.3) and per-group throughput (5.2) |
| 3 | Can `policy` and `accounting` absorb a 125× compressed burst? | owning teams, via Benhur | per-caller cutover |
| 4 | Is a 15-minute flexible window acceptable for the billing batch? | product / finance, via Benhur | the whole load-shaping decision |
| 5 | Production `ALTEOS_BATCH_SIZE` and `ALTEOS_CRON_TIME` | infra | the accuracy of every old-service figure here |
| 6 | Correct Scheduler execution-role trust policy shape | needs a trial harness — **not** settled | the spike, DPT-10338 |
| 7 | DynamoDB on-demand behaviour under a monthly cold peak | test, then infra | whether pre-warming or provisioned capacity is needed |
| 8 | Do the calling services actually retry a failed create, and with what key? | calling teams, via Benhur | whether the idempotent-create contract in section 2 is enough |
| 8 | The accounting drain timeline | that effort | decommissioning the old service |

Questions 1–3 should be answered before any per-caller cutover ticket is written. Question 2 is the
cheapest and unlocks the most.

---

## Appendix — how to reproduce

The PoC lives in a private repository (`bao-alteos/schedule-platform-poc`) and runs in a personal
AWS account, not the Alteos development account. It carries a console for driving it live, a demo
script, and a burst control (`count`, capped at 20) for reproducing the simultaneous-timer
measurements in 3.7 and 3.8.

The stack is destroyed when not in use, so figures in this document cannot be re-derived on demand
without redeploying it. Every measurement is dated 10.09.2026.
