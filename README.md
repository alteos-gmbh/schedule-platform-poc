# Schedule platform PoC

A working replacement for `services/schedule`, built from EventBridge Scheduler, Lambda, DynamoDB
and SQS, with a console so the thing can be watched rather than described.

Epic [DPT-10337](https://versicorp.atlassian.net/browse/DPT-10337). The design lives in
`services-monorepo/serverless/schedule/`; this folder is the proof, not the implementation.

## What it is meant to settle

The spike on 07.09.2026 answered questions about AWS. It did not answer the question the lead
actually asked, which is whether this architecture covers what the current service does. So this
PoC starts from the inbound surface instead of from the infrastructure:

- **All seven routes** of the old service, ported route for route, same fields, same required-ness,
  same response bodies where a caller might be matching on them.
- **Eleven behaviours that are not visible in the route list** and would each break a caller if
  missed. They are listed in [DEMO.md](DEMO.md) with the file and line they came from.
- **The failure mode the business case rests on**: a recurrence chain that ends silently. The old
  service marks a row `executed` and *then* creates the next occurrence, both inside one
  `try/catch` that only logs — so anything throwing between the two kills the chain permanently and
  leaves one log line behind. The PoC makes those two writes one DynamoDB transaction, and the
  console can run both orderings side by side.

## Not in it

- **No Kafka leg.** No caller consumes fired schedules over Kafka and the topic contract is
  unsettled. `messageTopicName` publishes to SQS here.
- **No ACL.** The old service guards four routes with `createAclV3Middleware` and leaves three
  unguarded. The design replaces all of it with IAM on the invoke, so there is nothing to port —
  but note that `GET /v1/schedule` and `POST /v1/schedule/activate` are currently unauthenticated,
  which the cutover has to decide about deliberately.
- **No production data.** Synthetic uuids only. A schedule context reaches the queue and the logs,
  and this account has none of the controls the Alteos accounts have. Questions that need real
  numbers go to the BI Champions group.

## Running it

Prerequisites: Terraform (1.5.7 via `tfenv` is what this was written against), Node 20 or newer,
and AWS credentials for **your own account** — not an Alteos one.

```sh
cd src && npm install && cd ..          # luxon and the AWS clients go into the zip
node src/test.mjs                       # 15 checks on the ported logic, no AWS needed

cd infra
terraform init
terraform apply -var expected_account_id=<your 12-digit account>

cd ..
npm install                             # the console's one dependency
npm run console                         # http://localhost:8787
```

Tear it down when the demo is over. Nothing is protected against deletion, deliberately:

```sh
cd infra && terraform destroy -var expected_account_id=<your account>
```

### Letting other people watch it

The page is static, but the proxy is the part that holds AWS credentials and makes the
`lambda:Invoke` calls — so there is nothing useful to "just host". Two ways round that.

**Screen share** needs no setup and is what the console was built for: four panels refreshing every
two seconds, and a scenario that lands in thirty seconds (create, drop timer, reconcile now).

**A tunnel** lets people click it themselves:

```sh
brew install cloudflared

# a token, so the URL alone is not enough
export POC_CONSOLE_TOKEN=$(node -e "console.log(require('crypto').randomBytes(16).toString('base64url'))")
npm run console &
cloudflared tunnel --url http://localhost:8787
```

The tunnel prints a `https://<random>.trycloudflare.com` URL. Hand round
`https://<random>.trycloudflare.com/?t=$POC_CONSOLE_TOKEN` — the token is needed once, then a
cookie carries it, so refreshes keep working.

`POC_CONSOLE_TOKEN` is only checked when it is set, so a laptop-only console stays as it was. It is
not authentication and is not pretending to be: it stops the URL from being sufficient on its own,
which is the entire requirement for a demo window. Anyone holding the link can create, cancel and
reset schedules in the account, because the demo surface is meant to let them — so treat the link
like a credential, and stop the tunnel when the demo is over.

The URL changes every time the tunnel restarts, the laptop has to stay awake, and closing the
terminal ends it. That is the trade for touching no infrastructure.

**Not a Lambda Function URL.** `AUTH_IAM` requires SigV4-signed requests, which a browser cannot
produce, so the safe setting is useless for this; and `AUTH_NONE` is a permanent public endpoint
with write access to the stack. It would also undercut the demo's own message, since the settled
design gives the real function no URL at all.

### Working from a second machine

The Terraform state is local and not in this repo — it names live resources and belongs to whoever
applied them. So a fresh checkout has no state, and `terraform apply` there would try to create a
stack that already exists and fail on the names.

To just watch the already-deployed stack, skip Terraform and point the console at the function:

```sh
cd poc && npm install
POC_FUNCTION_NAME=poc-schedule AWS_REGION=eu-central-1 npm run console
```

That needs AWS credentials for the account the stack is in, with `lambda:InvokeFunction` on the
function — nothing else, because every other setting lives on the function itself.

To own the stack from the second machine instead, either copy `infra/terraform.tfstate` across by
hand, or move the state to an S3 backend. For a PoC, copying it is the honest answer.

`node src/test.mjs` needs neither AWS nor Terraform and runs anywhere.

### A different IAM user in the same account

Nothing in the stack is tied to whoever created it. `data.aws_caller_identity` is read only for the
account id, no resource policy names a human principal — the only principals are
`scheduler.amazonaws.com` and `lambda.amazonaws.com` — and the account guard checks the account,
not the user. So a second IAM user in the same account is fine; the only question is what that user
is allowed to do.

**To watch the running stack**, one permission is the whole requirement. Everything else happens
inside the function under the function's own role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "WatchTheSchedulePoc",
      "Effect": "Allow",
      "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:eu-central-1:<account>:function:poc-schedule"
    }
  ]
}
```

**To run Terraform**, the user needs create-and-delete on `dynamodb`, `sqs`, `scheduler`, `lambda`,
`logs`, and — the part that is easy to miss — `iam:CreateRole`, `iam:PutRolePolicy` and
`iam:PassRole` on both `poc-schedule-role` and `poc-schedule-scheduler-role`. A user without
`iam:PassRole` fails at the point the Lambda is created, and the error names the action and the
role. Writing a least-privilege policy for this is more work than the PoC is worth; an
administrator user is the honest answer for a disposable sandbox.

**One trap.** `terraform destroy` with no state file destroys nothing. It reports success, and the
stack keeps running and keeps costing money. Whoever tears this down has to be the machine holding
`infra/terraform.tfstate`, or has to have copied it across first.

### The account guard

`expected_account_id` is passed to the provider's `allowed_account_ids`, so every AWS call fails
immediately if the shell's credentials point somewhere else. This workspace also holds the Alteos
Terraform repo; the guard is what keeps a wrong `AWS_PROFILE` from turning into resources in a
company account.

**`terraform apply` here is not the same act as `terraform apply` in `terraform/`.** That repo
routes every apply through Atlantis with DevOps approval and is never applied by hand. This stack
is standalone, personal, and disposable.

### Cost

Everything is on-demand or free-tier: a DynamoDB table with no provisioned capacity, three SQS
queues, one 512 MB Lambda, and a handful of schedules. A day of demoing costs cents. The reconciler
ticking every two minutes is the only thing that runs when nobody is watching — about 720 invokes a
day — so destroy the stack rather than leaving it idle for a week.

## Layout

| Path | What it is |
| --- | --- |
| `src/core.mjs` | Store, recurrence, validation, the outbound envelope, the Scheduler client |
| `src/dispatch.mjs` | The dispatcher and the reconciler |
| `src/index.mjs` | The handler, the four invocation guards, the seven routes, the demo surface |
| `src/test.mjs` | The ported logic, checked without AWS |
| `infra/` | The stack. One `main.tf`, applied and destroyed locally |
| `DEMO.md` / `DEMO.vi.md` | The demo script; the second is a Vietnamese translation of the first |
| `proxy.mjs` | Browser fetch to one `lambda:Invoke`, using your credentials |
| `ui/index.html` | The console, including the log panel |

## Why there is a proxy and not a URL

The design settles that the function carries no URL of any kind — no Function URL, no API Gateway —
and that management calls arrive as the payload of an SDK `lambda:Invoke` with IAM deciding who may
call. Adding a Function URL to get a demo would have demonstrated an architecture nobody agreed to.

So the console's proxy *is* a client: it composes the same HTTP-shaped payload a real caller
composes and invokes the function. Which means the UI demonstrates the invoke model rather than
sidestepping it, and switching between this PoC and the eventual dev-account deployment is a change
of one Terraform output.

## What this PoC also fixes, and what it measures

Two items commented on [terraform#608](https://github.com/alteos-gmbh/terraform/pull/608) are
implemented here rather than argued about:

- `aws_lambda_function_event_invoke_config` with an `on_failure` destination, plus `sqs:SendMessage`
  on the **function's** role. Scheduler invokes Lambda asynchronously, so a target that runs and
  throws is a delivered fire and never reaches the schedule's dead-letter queue.
- The Scheduler trust policy is **not** fixed here, and the PoC downgraded it from settled to open.
  Nine measured trials on 08.09.2026 killed three separate explanations for the
  `ValidationException: The execution role you provide must allow AWS EventBridge Scheduler to
  assume the role` failure, including the two-statement form being the cause. The failure is
  intermittent. `scheduler_trust_form` switches between the three candidate forms so a proper trial
  harness can be run against them; the table is in [DEMO.md](DEMO.md#11). Nobody should hand
  Benhur a recommendation before that runs.

Both also apply to the DLQ encryption CodeRabbit raised on that PR: the queues here declare
`sqs_managed_sse_enabled` rather than inheriting an account default.
