# Demo script

Vietnamese translation: [DEMO.vi.md](DEMO.vi.md). This file is the source of truth.

Eleven behaviours to cover and eleven things to show. Every claim about the old service has a file
and line behind it, so a disagreement in the room can be settled by opening the file.

Start the console (`npm run console`), open <http://localhost:8787>, press **reset everything**.

The create form shows only what a room needs to follow: when it fires, how often, and the policy
it belongs to. Shape, `messageTopicName` and `context.command` are still there and still drive the
call, just hidden — every visible field is one more thing to explain. The right-hand panel is the
function's own log, so the line that changed a row sits beside the row changing.

---

## The inbound surface being replaced

| Route | Auth in the old service | Ported |
| --- | --- | --- |
| `GET /v1/health` | none | yes |
| `GET /v1/schedule` | **none** | yes |
| `POST /v1/schedule` | ACL `ScheduleCreateScheduledActions` | yes, IAM instead |
| `POST /v1/schedule/cancel` | ACL `ScheduleCancelScheduledActions` | yes, IAM instead |
| `POST /v1/schedule/activate` | **none** | yes |
| `POST /v2/schedule` | ACL create | yes, IAM instead |
| `DELETE /v2/schedule` | ACL cancel | yes, IAM instead |

Plus two in-process cron jobs — `executeScheduledActions` and `resetLockedScheduledActions`
(`createCron.ts`) — which become the Scheduler timer and the reconciler.

## The eleven behaviours no route list shows

| # | Behaviour | Where it lives in the old service |
| --- | --- | --- |
| 1 | Anti-drift recurrence: `beginAt + period × (counter+1)`, not `lastTrigger + period` | `processors/calculateNextTriggerDate.ts` |
| 2 | `endAt` compared at **day** granularity, `startOf('day') > endOf('day')` | `processors/executeScheduledActions.ts:206`, `:289` |
| 3 | …and that comparison resolves in the **process time zone**, which nothing pins | `convertToDateTime` in `@alteos-gmbh/common` is a bare `DateTime.fromISO` |
| 4 | V1 vs V2 chosen by `messageTopicName` being absent, not by a version field | `processors/executeScheduledActions.ts:130` |
| 5 | V1 mints its own `authorizationData` and moves `partnerId` to `scopePartnerId` | `processors/executeScheduledActions.ts` V1 branch |
| 6 | A `.fifo` topic runs sequentially with `messageGroupId = context.policyId` | `processors/executeScheduledActions.ts:175` |
| 7 | Cancel matches `context.command` **or** `context.name`, pending rows only | `processors/cancelScheduledActions.ts:31`, `:54` |
| 8 | `context.__waitForApproval` parks a V1 row at `waitingExecutionApproval`; V2 ignores it | `api/createScheduledActionHandler.ts:19`, `:48-55` |
| 9 | `processingData` is written whenever `period !== null` — so an **absent** period gets one too | `api/createScheduledActionHandler.ts:61`, `api/createScheduledActionV2Handler.ts:54` |
| 10 | Six statuses, not four: `preExecuted` and `waitingExecutionApproval` exist | `common/ScheduledActionStatus.ts` |
| 11 | A row stuck `processing` is only recovered by a second cron job after `PROCESSING_TTL` | `processors/resetLockedScheduledActions.ts:19` |

`node src/test.mjs` checks 1, 2, 3, 5, 6, 8 and 9 without touching AWS. The rest are shown live.

---

## 1 — A recurrence chain runs

Create with **period `PT1M`**, fires in 60 seconds — both are the form defaults, so this is one click.

Watch: the row goes `pending` → `processing` → `executed`, a **new** row appears `pending` with
`counter` incremented, and a message lands in **Fired** carrying `authorizationData` with
`scopePartnerId` set and `partnerId` null — behaviour 5.

Say: DynamoDB holds the chain, the Scheduler holds one timer at a time.

## 2 — The chain does not drift

The counter is visible in the table; the month arithmetic is not observable in a demo, so it is a
test instead. `node src/test.mjs` shows `2026-01-31 + P1M → 02-28`, and then hop two landing back
on `03-31` rather than `03-28`. Behaviour 1.

The same test shows behaviour 3: the same two instants read in UTC+7 give a different answer, so
the old service's chain length depends on the container's `TZ`. The PoC's Lambda declares `TZ=UTC`.

## 2b — Scheduler's delivery latency, and why `PT1M` is the floor

Measured 09.09.2026, `FlexibleTimeWindow` set to `OFF`, so none of this is configured jitter.

**The clean measurement**, a `PT1M` chain where nothing was clamped, so each fire is timed against
the schedule's own `at()`:

| counter | computed triggerAt | actually fired | late by |
| --- | --- | --- | --- |
| c0 | 03:59:01 | 03:59:27–03:59:33 | 26–32s |
| c1 | 04:00:01 | 04:00:12–04:00:20 | 11–19s |
| c2 | 04:01:01 | 04:01:05–04:01:12 | 4–11s |

So latency is **4–32 seconds and highly variable**, and in this run it shrank on each hop. Do not
quote a tight figure from this — six fires across two runs is not a distribution, and an earlier
version of this section recorded "25–37s", which was inferred from clamped timers rather than
measured and was too narrow.

The chain itself is stable: all four occurrences landed on `:01` seconds exactly 60 apart, so
`PT1M` neither drifts nor clamps. The next occurrence is still ~25s in the future when a fire
happens, which is what keeps it that way — and it doubles as the live proof of behaviour 1 at a
cadence a room can watch.

**Below a minute it falls apart.** The same setup at `PT15S`:

| counter | computed triggerAt | actually fired |
| --- | --- | --- |
| c0 | 03:42:52 | 03:43:32–03:43:37 |
| c1 | 03:43:07 | 03:44:10–03:44:16 |
| c2 | 03:43:22 | 03:44:54–03:45:00 |

The arithmetic is still exact — 52, 07, 22, fifteen seconds apart — but every next occurrence is
already in the past when it is computed, gets pushed out to `now + min_lead_seconds`, and the gap
between actual fires becomes ~38s then ~44s. The chain is permanently catching up, which reads on
screen as a runaway.

The design conclusion holds regardless of the exact numbers: **this platform cannot deliver
sub-minute precision.** No current caller needs it. But it is an architectural limit nobody has
written down, and finding it after a cutover would be too late. Worth reading production's
`ALTEOS_CRON_TIME` to see whether the old service's cron is tighter or looser than this.

## 3 — The chain ends silently. This is the business case.

Turn on **legacy ordering** and **break chain write**. Create with period `PT1M`.

When it fires: the row goes **`executed`**, no next row is created, the timers list empties, and
**nothing appears in the dead-letter queue**. The only trace is one log line
(`chainBrokenSilently`). That policy has stopped being billed and no alarm exists.

This is `executeScheduledActions.ts`: `:182` marks the row executed, `:229`/`:232` create the next
occurrence, and `:236` catches whatever throws between them and logs it.

## 4 — The same failure, with the ordering fixed

Turn **legacy ordering off**, leave **break chain write** on. Create with period `PT1M`.

When it fires: the transaction fails, so the row stays **`processing`** with `attempts` climbing
and `lastError` filled in. Lambda retries twice — the retries re-enter the same row deliberately,
because refusing a `processing` row would make attempt two *succeed* and the failure would vanish
again — and after the third failure a record appears in **Dead letters**, through the
`aws_lambda_function_event_invoke_config` destination. After a minute the reconciler moves the
row back to `pending` and re-creates its timer.

Nothing was lost, and the failure is visible in three places instead of none.

One artefact of the demo settings, not of the design: `processing_ttl_minutes` and the reconciler
tick are both 1 minute here, while Lambda's own async retries are spread over several minutes with
backoff. So the reconciler usually takes the row back to `pending` *while* Lambda is still retrying
it, and the dead-letter record lands after the row already looks healthy. The design's 15-minute
tick sits well outside Lambda's retry window and does not race. Worth saying out loud, because the
order on screen is not the order production would produce.

The exposure this leaves, which belongs in the design discussion rather than in a footnote: a fire
that published and *then* failed publishes again on every retry. Measured 08.09.2026 — this exact
scenario put **three** copies on the target queue for one schedule, and the dead-letter record
says so:

```
requestPayload  {"scheduleId":"8f0153e3-29f7-4b79-9481-bcb796c42578"}
condition       RetriesExhausted   approximateInvokeCount 3
errorMessage    simulated chain-write failure (POC breakNext)
```

`attempts` on the row is the same count. At-least-once is also what the old service gives, so this
is not a regression — but it does mean every consumer of a fired schedule has to be idempotent, and
nobody has checked that they are. Worth a ticket.

## 5 — A publish failure

Turn on **break publish** only. Same shape as 4, with the error coming from the queue write. Shows
that both failure paths land in the same place.

## 6 — Cancel

Create one, then press **cancel policy** — `DELETE /v2/schedule?policyId=`. The row goes
`cancelled` and the timer disappears from the Scheduler list. Both writes, or neither.

**cancel command** exercises `POST /v1/schedule/cancel`, matching on `context.command` or
`context.name`. Behaviour 7.

## 7 — The approval gate

Create with **`__waitForApproval`** ticked. The row appears as `waitingExecutionApproval` and the
timer column shows `—`: no timer at all, so it cannot fire. Press **activate** and it becomes
`pending` with a timer.

Behaviours 8 and 10. Worth noting in the room: `activate` in the old service updates by
`id IN (...)` with **no status condition**, so activating an already-`cancelled` row resurrects it.
Reproduced; almost certainly not intended.

## 8 — The reconciler repairs towards the row

On a `pending` row press **drop timer**. The timer column flips to `MISSING` — the timer is gone
from AWS while the intent still stands. Press **reconcile now** and it comes back.

This is what replaces `resetLockedScheduledActions` and does considerably more than it.

## 9 — …and deletes what the row does not want

On a `pending` row press **drop row**. The timer is now an `ORPHAN`. Press **reconcile now** and it
is deleted. If it fires first, the dispatcher publishes nothing and logs `orphanFire`.

Together: every disagreement resolves in the direction of the DynamoDB row.

## 10 — Idempotency

Press **activate** twice on the same row. The second `CreateSchedule` hits the existing name,
answers `ConflictException`, and is read as proof the first attempt landed — no overwrite, no
duplicate fire. A `ClientToken` changes nothing and is not sent. Measured 07.09.2026.

## 11 — The trust policy question on terraform#608, and why it is still open

```sh
cd infra
terraform apply -var expected_account_id=<account> -var scheduler_trust_form=two_statement
terraform apply -var expected_account_id=<account> -var scheduler_trust_form=source_arn_only
terraform apply -var expected_account_id=<account>   # account_only, the default
```

**Do not present this as settled.** What happened on 08.09.2026 in account 839810213476,
eu-central-1, in order:

| # | What ran | Trust form | Result |
| --- | --- | --- | --- |
| 1 | Terraform, first apply, role seconds old | two_statement | **failed** after ~1m50s of retries |
| 2 | Terraform, `ArnLike` widened to `scheduler:*` | — | created in 7s |
| 3 | Terraform, `ArnLike` on the group ARN | — | created in 1s |
| 4 | Terraform, group ARN only | — | created in 2s |
| 5 | Terraform, `schedule/<group>/*` only — the failing form from #1 | source_arn_only | created in 0s |
| 6 | Terraform, role fully recreated (`-replace`) | source_arn_only | created in 3s |
| 7 | Lambda, three calls in a row, brand-new schedule names | two_statement | **failed** all three |
| 8 | Lambda, brand-new name | account_only | created |
| 9 | Lambda, brand-new name | two_statement | created |

Three hypotheses were tried and each was killed by the next row: the condition shape (killed by 5),
the age of the role (killed by 6), and the schedule name being new (killed by 9).

What can be said: the error is real, it is intermittent, and it stopped occurring after the trust
policy was rewritten. What cannot be said: which form is correct, or that the failure is not
eventual consistency — the #608 comment asserts it is not, and rows 1 and 7 versus 9 do not support
that assertion.

Settling it needs a clean trial harness — fresh role, fresh schedule name, N repetitions per form,
counting failures — not another one-off. Roughly two minutes per failing trial. Until that runs,
`account_only` is the default here because it is the form with the fewest recorded failures, and
`aws:SourceAccount` alone still confines the role to this account's Scheduler.

## A finding worth carrying into the implementation

Scheduler rejects an `at()` expression carrying fractional seconds:

```
Invalid Schedule Expression at(2026-09-08T03:54:26.439).
```

luxon's `suppressMilliseconds` only drops them when they are already zero, so it is not a fix —
`toFormat("yyyy-MM-dd'T'HH:mm:ss")` is. Every `triggerAt` arriving from a caller has milliseconds,
so this would have hit the real implementation on its first call.

## FIFO ordering, if asked

Set **messageTopicName** to `policy-obligations.fifo`. Fires route to the FIFO queue with
`messageGroupId = policyId`, which is how per-policy ordering survives. Behaviour 6.

---

## What a viewer should not conclude

- This is not the implementation. It is plain JavaScript in a personal account; the real thing is
  TypeScript in `serverless/schedule` on infrastructure from `terraform#608`.
- The cancel path falls back to a table scan for any criterion other than `policyId`. Fine at PoC
  row counts, not fine in production — DPT-10343 has to choose an index or a restriction.
- Nothing here has been load-tested, and Scheduler's per-account schedule quota has not been
  measured against the number of live policies. That is a real open question, not a solved one.
- The trust-policy form is unsettled — see 11. Three explanations were tried and each was
  disproved by the next measurement.
- A retried fire can publish twice. See 4.
