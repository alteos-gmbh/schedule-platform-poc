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
  description = "Reconciler tick. 2 minutes so a demo does not stall; the design says 15."
  type        = number
  default     = 2
}

variable "processing_ttl_minutes" {
  description = "How long a row may sit in `processing` before the reconciler takes it back."
  type        = number
  default     = 2
}

variable "log_retention_days" {
  description = "Explicit retention. The real function relies on the Datadog extension instead."
  type        = number
  default     = 7
}
