# The schedule platform PoC — DynamoDB, EventBridge Scheduler, Lambda and SQS in one stack.
#
# THIS IS NOT THE ALTEOS TERRAFORM REPO. That repo branches off master, is applied only through
# Atlantis with DevOps approval, and `terraform apply` there is never run by hand. This stack is
# standalone, lives in a personal AWS account, and is meant to be applied and destroyed locally.
# The provider's `allowed_account_ids` makes the distinction enforceable rather than a convention.
#
#   terraform init
#   terraform apply  -var expected_account_id=<your account>
#   terraform destroy -var expected_account_id=<your account>
#
# Nothing here is protected against deletion, on purpose: a PoC that is awkward to tear down gets
# left running. The real stack (terraform#608) sets the opposite defaults.

terraform {
  required_version = "~> 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}

provider "aws" {
  region = var.region

  # The guard. Any call against a different account fails before it does anything.
  allowed_account_ids = [var.expected_account_id]

  default_tags {
    tags = {
      Project   = "schedule-platform-poc"
      Ticket    = "DPT-10337"
      Ephemeral = "true"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  group_name = "${var.name_prefix}-group"
  fn_name    = var.name_prefix

  group_arn = "arn:aws:scheduler:${var.region}:${local.account_id}:schedule-group/${local.group_name}"
  # Every schedule inside the group, which is what a fire's aws:SourceArn looks like.
  schedule_arn_pattern = "arn:aws:scheduler:${var.region}:${local.account_id}:schedule/${local.group_name}/*"
}

# ------------------------------------------------------------------------------------------------
# The store. Source of truth for intent and state; the Scheduler holds no business state at all.
# ------------------------------------------------------------------------------------------------

resource "aws_dynamodb_table" "schedule" {
  name         = "${var.name_prefix}-table"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "id"

  attribute {
    name = "id"
    type = "S"
  }

  attribute {
    name = "policyId"
    type = "S"
  }

  attribute {
    name = "status"
    type = "S"
  }

  attribute {
    name = "triggerAt"
    type = "S"
  }

  # The old service queries `context.policyId` inside a JSONB column. DynamoDB cannot index into a
  # map, so the PoC lifts policyId onto the item and indexes that — the one shape change the move
  # to DynamoDB forces on every read path.
  global_secondary_index {
    name            = "byPolicy"
    hash_key        = "policyId"
    range_key       = "triggerAt"
    projection_type = "ALL"
  }

  # What the dispatcher and the reconciler sweep: pending work in trigger order.
  global_secondary_index {
    name            = "byStatus"
    hash_key        = "status"
    range_key       = "triggerAt"
    projection_type = "ALL"
  }
}

# ------------------------------------------------------------------------------------------------
# Delivery. Two target queues because a `.fifo` topic name means ordered per policy in the old
# service, and one dead-letter queue serving two different failure modes.
# ------------------------------------------------------------------------------------------------

resource "aws_sqs_queue" "target" {
  name = "${var.name_prefix}-target"
  # A schedule context can hold customer data, so the queue that carries it declares encryption
  # rather than inheriting whatever the account default happens to be.
  sqs_managed_sse_enabled = true
}

resource "aws_sqs_queue" "target_fifo" {
  name                        = "${var.name_prefix}-target.fifo"
  fifo_queue                  = true
  content_based_deduplication = false
  sqs_managed_sse_enabled     = true
}

resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name_prefix}-dlq"
  message_retention_seconds = 1209600 # 14 days, the maximum
  sqs_managed_sse_enabled   = true
}

# ------------------------------------------------------------------------------------------------
# The timer
# ------------------------------------------------------------------------------------------------

resource "aws_scheduler_schedule_group" "group" {
  name = local.group_name
}

# ------------------------------------------------------------------------------------------------
# IAM — the role Scheduler assumes to invoke the function
# ------------------------------------------------------------------------------------------------

data "aws_iam_policy_document" "scheduler_trust" {
  # Statement one: the fire itself, which does carry an aws:SourceArn naming the schedule.
  statement {
    sid     = "AllowSchedulerFire"
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["scheduler.amazonaws.com"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [local.account_id]
    }

    # Dropped entirely under "account_only": SourceAccount alone still confines the role to this
    # account's Scheduler, and it is the only form measured to let a new schedule name through.
    dynamic "condition" {
      for_each = var.scheduler_trust_form == "account_only" ? [] : [1]

      content {
        test     = "ArnLike"
        variable = "aws:SourceArn"
        values   = [local.schedule_arn_pattern]
      }
    }
  }

  # Statement two: the form proposed on terraform#608 for whatever CreateSchedule checks before the
  # schedule exists.
  dynamic "statement" {
    for_each = var.scheduler_trust_form == "two_statement" ? [1] : []

    content {
      sid     = "AllowSchedulerPreCreateProbe"
      actions = ["sts:AssumeRole"]

      principals {
        type        = "Service"
        identifiers = ["scheduler.amazonaws.com"]
      }

      condition {
        test     = "StringEquals"
        variable = "aws:SourceAccount"
        values   = [local.account_id]
      }

      condition {
        test     = "Null"
        variable = "aws:SourceArn"
        values   = ["true"]
      }
    }
  }
}

resource "aws_iam_role" "scheduler" {
  name               = "${var.name_prefix}-scheduler-role"
  assume_role_policy = data.aws_iam_policy_document.scheduler_trust.json
}

data "aws_iam_policy_document" "scheduler" {
  statement {
    sid       = "InvokeTheFunction"
    actions   = ["lambda:InvokeFunction"]
    resources = [aws_lambda_function.schedule.arn]
  }

  # Scheduler writes its own dead-letter records under this role: the ones for fires it could not
  # deliver at all. A fire that reached the function and threw is not one of them.
  statement {
    sid       = "SchedulerDeadLetter"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }
}

resource "aws_iam_role_policy" "scheduler" {
  name   = "${var.name_prefix}-scheduler"
  role   = aws_iam_role.scheduler.id
  policy = data.aws_iam_policy_document.scheduler.json
}

# ------------------------------------------------------------------------------------------------
# IAM — the function's own role
# ------------------------------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_trust" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name_prefix}-role"
  assume_role_policy = data.aws_iam_policy_document.lambda_trust.json
}

data "aws_iam_policy_document" "lambda" {
  statement {
    sid = "Logs"
    actions = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
    ]
    resources = ["${aws_cloudwatch_log_group.schedule.arn}:*"]
  }

  # The console shows the function's own log lines beside the rows they changed, so the function
  # reads its own group back. Read-only, and scoped to that one group.
  statement {
    sid       = "ReadOwnLogsForTheDemo"
    actions   = ["logs:FilterLogEvents"]
    resources = ["${aws_cloudwatch_log_group.schedule.arn}:*"]
  }

  statement {
    sid = "Store"
    actions = [
      "dynamodb:GetItem",
      "dynamodb:PutItem",
      "dynamodb:UpdateItem",
      "dynamodb:DeleteItem",
      "dynamodb:Query",
      "dynamodb:Scan",
      "dynamodb:TransactWriteItems",
    ]
    resources = [
      aws_dynamodb_table.schedule.arn,
      "${aws_dynamodb_table.schedule.arn}/index/*",
    ]
  }

  # Publish to the two target queues, and — the part terraform#608 is missing — send to the DLQ.
  # Lambda writes a failure destination record under the *function's* role, not Scheduler's. With
  # this statement absent nothing arrives on the queue and nothing says why.
  statement {
    sid     = "Publish"
    actions = ["sqs:SendMessage"]
    resources = [
      aws_sqs_queue.target.arn,
      aws_sqs_queue.target_fifo.arn,
      aws_sqs_queue.dlq.arn,
    ]
  }

  # The demo surface reads the queues back so a browser has something to show.
  statement {
    sid = "ReadBackForTheDemo"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [
      aws_sqs_queue.target.arn,
      aws_sqs_queue.target_fifo.arn,
      aws_sqs_queue.dlq.arn,
    ]
  }

  statement {
    sid = "ManageTimers"
    actions = [
      "scheduler:CreateSchedule",
      "scheduler:DeleteSchedule",
      "scheduler:GetSchedule",
      "scheduler:UpdateSchedule",
    ]
    resources = ["${local.schedule_arn_pattern}"]
  }

  statement {
    sid       = "ListTimers"
    actions   = ["scheduler:ListSchedules"]
    resources = ["*"]
  }

  # Required, and the error when it is missing names both the action and the role.
  statement {
    sid       = "PassTheSchedulerRole"
    actions   = ["iam:PassRole"]
    resources = [aws_iam_role.scheduler.arn]
  }
}

resource "aws_iam_role_policy" "lambda" {
  name   = var.name_prefix
  role   = aws_iam_role.lambda.id
  policy = data.aws_iam_policy_document.lambda.json
}

# ------------------------------------------------------------------------------------------------
# The function
# ------------------------------------------------------------------------------------------------

data "archive_file" "bundle" {
  type        = "zip"
  source_dir  = "${path.module}/../src"
  output_path = "${path.module}/.terraform/schedule-poc.zip"
}

# The real function declares no log group: the Datadog extension ships its logs. The PoC has no
# extension, so it needs one, with retention set rather than left unbounded.
resource "aws_cloudwatch_log_group" "schedule" {
  name              = "/aws/lambda/${local.fn_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_lambda_function" "schedule" {
  function_name = local.fn_name
  role          = aws_iam_role.lambda.arn
  handler       = "index.handler"
  runtime       = "nodejs22.x"
  architectures = ["arm64"]
  timeout       = 30
  memory_size   = 512

  filename         = data.archive_file.bundle.output_path
  source_code_hash = data.archive_file.bundle.output_base64sha256

  environment {
    variables = {
      POC_TABLE                  = aws_dynamodb_table.schedule.name
      POC_SCHEDULE_GROUP         = aws_scheduler_schedule_group.group.name
      POC_QUEUE_URL              = aws_sqs_queue.target.url
      POC_FIFO_QUEUE_URL         = aws_sqs_queue.target_fifo.url
      POC_DLQ_URL                = aws_sqs_queue.dlq.url
      POC_SCHEDULER_ROLE_ARN     = aws_iam_role.scheduler.arn
      POC_FUNCTION_ARN           = "arn:aws:lambda:${var.region}:${local.account_id}:function:${local.fn_name}"
      POC_PROCESSING_TTL_MINUTES = tostring(var.processing_ttl_minutes)
      POC_MAX_DELIVERY_ATTEMPTS  = tostring(var.max_delivery_attempts)
      # Lambda never tells a function which retry it is on, so the function counts for itself and
      # needs to know where the count ends.
      POC_INVOCATIONS_PER_FIRING = tostring(var.lambda_retry_attempts + 1)
      POC_LOG_GROUP              = aws_cloudwatch_log_group.schedule.name
      # Declared, not inherited. The endAt gate resolves startOf('day') in the process zone, and
      # the old service pins nothing — so the boundary silently follows whatever TZ the container
      # was started with. See the test in ../src/test.mjs.
      TZ = "UTC"
    }
  }

  depends_on = [
    aws_iam_role_policy.lambda,
    aws_cloudwatch_log_group.schedule,
  ]
}

# The measured correction to the RFC, and blocker 1 on terraform#608.
#
# Scheduler invokes Lambda asynchronously (InvocationType: Event), so Lambda answers 202 at once
# and a target that runs and then throws is a *delivered* fire. Scheduler's own dead-letter queue
# never sees it. Only Lambda's failure destination does, and it needs both this block and
# sqs:SendMessage on the function's role — both of which the RFC omits.
resource "aws_lambda_function_event_invoke_config" "schedule" {
  function_name          = aws_lambda_function.schedule.function_name
  maximum_retry_attempts = var.lambda_retry_attempts

  destination_config {
    on_failure {
      destination = aws_sqs_queue.dlq.arn
    }
  }
}

# ------------------------------------------------------------------------------------------------
# The reconciler's own tick — the only schedule Terraform owns
# ------------------------------------------------------------------------------------------------

resource "aws_scheduler_schedule" "reconciler" {
  name       = "${var.name_prefix}-reconciler"
  group_name = aws_scheduler_schedule_group.group.name

  schedule_expression          = "rate(${var.reconciler_rate_minutes} minutes)"
  schedule_expression_timezone = "UTC"

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = aws_lambda_function.schedule.arn
    role_arn = aws_iam_role.scheduler.arn

    # Verbatim, not wrapped. Scheduler passes a target's input through untouched, unlike an
    # EventBridge *rule* — so the tick arrives as exactly this object and the guard tests
    # `source === "reconciler"`, never "aws.events".
    input = jsonencode({ source = "reconciler" })

    retry_policy {
      maximum_retry_attempts       = 2
      maximum_event_age_in_seconds = 900
    }

    dead_letter_config {
      arn = aws_sqs_queue.dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.scheduler]
}

# ------------------------------------------------------------------------------------------------
# Outputs — `terraform output -json` is what the local proxy reads, so nothing is wired by hand.
# ------------------------------------------------------------------------------------------------

output "function_name" {
  value = aws_lambda_function.schedule.function_name
}

output "region" {
  value = var.region
}

output "table_name" {
  value = aws_dynamodb_table.schedule.name
}

output "schedule_group" {
  value = aws_scheduler_schedule_group.group.name
}

output "dlq_url" {
  value = aws_sqs_queue.dlq.url
}

output "account_id" {
  value = local.account_id
}
