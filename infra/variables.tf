variable "expected_account_id" {
  description = <<-DESC
    The AWS account this stack is allowed to touch, passed to the provider's own account guard.

    This is the whole safety mechanism, and it is not decoration. The workspace this PoC lives in
    also holds the Alteos infrastructure repo; if the shell's credentials ever point at a company
    account, every AWS call here fails immediately rather than creating resources somewhere they do
    not belong.
  DESC
  type        = string

  validation {
    condition     = can(regex("^[0-9]{12}$", var.expected_account_id))
    error_message = "expected_account_id must be a 12-digit AWS account id."
  }
}

variable "region" {
  description = "Region for the PoC. eu-central-1 matches where the real platform will run."
  type        = string
  default     = "eu-central-1"
}

variable "name_prefix" {
  description = "Prefix on every resource, so one destroy finds everything this PoC made."
  type        = string
  default     = "poc-schedule"
}

variable "scheduler_trust_form" {
  description = <<-DESC
    Which trust-policy form the Scheduler execution role carries. This is the open question on
    terraform#608 and the reason the variable exists rather than a hardcoded policy.

      "two_statement"   ArnLike aws:SourceArn on schedule/<group>/*, plus a second statement
                        conditioned on Null aws:SourceArn = "true". The form proposed on #608
                        and never proven.
      "source_arn_only" ArnLike aws:SourceArn alone. The form #608 currently carries.
      "account_only"    aws:SourceAccount alone, no SourceArn condition. The form the #608
                        comment measured as working.

    Every form also keeps aws:SourceAccount, so none of them lets another account's Scheduler use
    the role.

    Read carefully before trusting any measurement taken with this: a CreateSchedule for a schedule
    NAME THAT ALREADY EXISTED behaves differently from one for a new name. Re-creating the
    Terraform-owned reconciler succeeds under every form; only a brand-new name exercises whatever
    check produces

      ValidationException: The execution role you provide must allow AWS EventBridge Scheduler
      to assume the role.

    So a valid trial creates a name that has never existed in the account, which is what the
    Lambda's own CreateSchedule does on every management call.
  DESC
  type        = string
  default     = "account_only"

  validation {
    condition = contains(
      ["two_statement", "source_arn_only", "account_only"],
      var.scheduler_trust_form
    )
    error_message = "scheduler_trust_form must be two_statement, source_arn_only or account_only."
  }
}

variable "reconciler_rate_minutes" {
  description = <<-DESC
    Reconciler tick. 1 minute, which is the smallest interval `rate()` accepts, so nobody waits on
    it during a demo. The design says 15.
  DESC
  type        = number
  default     = 1
}

variable "processing_ttl_minutes" {
  description = <<-DESC
    How long a row may sit in `processing` before the reconciler treats the firing as lost.

    **This must exceed Lambda's whole async retry window, and the reason is not obvious.** The
    dispatcher already ends a firing itself: on the final attempt it parks the row at `failed` and
    still throws, so the failure destination records it. The reconciler exists only for a firing the
    handler never got to observe at all — a timeout, an out-of-memory kill — which is the case
    docs/consistency.md says the ceiling cannot see.

    If this is shorter than the retry window, the reconciler parks the row while Lambda is still
    retrying. The next retry then finds a row it cannot claim, returns success, and Lambda concludes
    the invocation succeeded — so **no dead-letter record is ever written** and the failure vanishes
    silently. Measured: at 1 minute, a broken target produced `failed` with `attempts` stuck at 2
    and an empty queue.

    Lambda spreads two async retries over roughly three minutes, so five is the smallest honest
    value. It is also what the design uses for its own stale sweep.
  DESC
  type        = number
  default     = 5

  validation {
    condition     = var.processing_ttl_minutes >= 4
    error_message = "Must exceed Lambda's async retry window (~3 minutes) or dead-letter records are silently suppressed."
  }
}

variable "min_lead_seconds" {
  description = <<-DESC
    How far ahead of now a timer must be before Scheduler is asked for it. A `triggerAt` nearer than
    this is pushed out to it; anything further away is used exactly as given.

    The floor exists for repair — the reconciler re-creates timers for occurrences whose time has
    already passed, and `at()` in the past is not a documented shape. 10 seconds is enough to cover
    the CreateSchedule round trip while leaving a sub-minute `period` such as `PT15S` working as
    written, which a demo needs and a minute-long floor quietly prevented.
  DESC
  type        = number
  default     = 10
}

variable "lambda_retry_attempts" {
  description = <<-DESC
    Lambda's own async retries after the first invocation, so total invocations per firing is this
    plus one. At 1 a firing is two attempts — attempt one fails, attempt two fails, and the failure
    destination writes one dead-letter record.

    At 2 — the default, and what the dead-letter record reports as
    `approximateInvokeCount: 3` — a firing is three attempts before `RetriesExhausted`.

    This is the number a viewer counts on screen as `attempts` climbing on the row, because the
    dispatcher records one failure per invocation.
  DESC
  type        = number
  default     = 2

  validation {
    condition     = var.lambda_retry_attempts >= 0 && var.lambda_retry_attempts <= 2
    error_message = "Lambda allows 0, 1 or 2 async retries."
  }
}

variable "max_delivery_attempts" {
  description = <<-DESC
    How many times the platform will try to deliver one occurrence before parking it at `failed`.

    A delivery is one firing. Each firing is already three Lambda invocations — the initial one plus
    `maximum_retry_attempts = 2` — and ends in one dead-letter record, so this counts firings and
    not invocations. The row's `attempts` field counts the other thing on purpose.

    At 1: the occurrence fires once, `lambda_retry_attempts + 1` invocations happen, one dead-letter
    record is written, and the reconciler then parks the row at `failed` rather than firing it
    again. Raise it to re-fire an exhausted occurrence that many times, each firing producing its
    own dead-letter record.

    The `failed` status is the design's, not this PoC's: docs/consistency.md has the dispatcher park
    a row there and terraform#608 indexes `byStatus` for "admin queries for `failed`". What the old
    service lacks is any cap at all — `resetLockedScheduledActions` returns a stuck row to `pending`
    for ever.

    The design's own number is different from this one and counts something else: three consecutive
    *exhausted firings*, tracked in `executions`, after which the dispatcher also disables the
    schedule and alerts. Three firings is up to nine invocations. This variable counts firings too,
    but defaults to 1 because a demo should not need nine failures to make a point.
  DESC
  type        = number
  default     = 1
}

variable "log_retention_days" {
  description = "Explicit retention. The real function relies on the Datadog extension instead."
  type        = number
  default     = 7
}
