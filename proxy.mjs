/**
 * The local console for the PoC.
 *
 * The design settles that the schedule platform carries no URL of any kind — no Function URL, no
 * API Gateway — and that an SDK `lambda:Invoke` carrying an HTTP-shaped payload is the management
 * transport, with IAM deciding who may call. A PoC that bolted a Function URL on to get a demo
 * would be demonstrating an architecture nobody agreed to.
 *
 * So the browser talks to this process, and this process is the caller: it composes the same
 * payload a real client composes and invokes the function with the operator's own credentials. The
 * UI ends up proving the invoke model rather than working around it.
 *
 *   node proxy.mjs            # http://localhost:8787
 *
 * Configuration comes from `terraform output -json`, so there is nothing to keep in sync by hand.
 */

import { execFileSync } from 'node:child_process';
import { readFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import {
  InvokeCommand,
  LambdaClient,
} from '@aws-sdk/client-lambda';

const here = dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT ?? 8787);

/**
 * Shared token, required only when set.
 *
 * On a laptop the console is reachable only from that laptop, so nothing needs guarding and the
 * default is no gate. Behind a tunnel it is a different situation entirely: the URL is public while
 * the tunnel is open, this process holds AWS credentials, and the demo surface can create, cancel
 * and reset schedules. So a tunnelled console sets POC_CONSOLE_TOKEN, and the link handed round
 * carries `?t=<token>` once — after that a cookie carries it, so a refresh keeps working.
 *
 * This is not authentication. It stops a URL from being enough on its own, which for a demo window
 * is the whole requirement.
 */
const TOKEN = process.env.POC_CONSOLE_TOKEN ?? '';

const outputs = readTerraformOutputs();
const lambda = new LambdaClient({ region: outputs.region });

console.log(
  `schedule PoC console\n` +
    `  account  ${outputs.account_id}\n` +
    `  region   ${outputs.region}\n` +
    `  function ${outputs.function_name}\n` +
    `  console  http://localhost:${PORT}\n`
);

createServer(async (request, response) => {
  try {
    if (!authorised(request, response)) return;

    if (request.url.split('?')[0] === '/' || request.url.startsWith('/index.html')) {
      const page = await readFile(join(here, 'ui', 'index.html'));
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      response.end(page);
      return;
    }

    if (request.url.split('?')[0] === '/config') {
      return send(response, 200, {
        account: outputs.account_id,
        region: outputs.region,
        functionName: outputs.function_name,
        table: outputs.table_name,
        group: outputs.schedule_group,
      });
    }

    if (request.url.startsWith('/api/')) {
      return await forward(request, response);
    }

    send(response, 404, { error: 'not found' });
  } catch (error) {
    send(response, 500, { error: error.message });
  }
}).listen(PORT);

/**
 * The token gate. Accepts the token from `?t=`, from a cookie, or from an `x-poc-token` header,
 * and sets the cookie so the query string is needed only for the first request.
 */
function authorised(request, response) {
  if (!TOKEN) return true;

  const url = new URL(request.url, 'http://localhost');
  const fromQuery = url.searchParams.get('t');
  const fromHeader = request.headers['x-poc-token'];
  const fromCookie = String(request.headers.cookie ?? '')
    .split(';')
    .map((part) => part.trim())
    .find((part) => part.startsWith('poc_token='))
    ?.slice('poc_token='.length);

  if (fromQuery === TOKEN) {
    // HttpOnly so page scripts cannot read it back out; SameSite=Lax is enough for a link people
    // open directly. No Secure flag decision to make — a tunnel is always https.
    response.setHeader(
      'set-cookie',
      `poc_token=${TOKEN}; HttpOnly; SameSite=Lax; Secure; Path=/; Max-Age=28800`
    );
    return true;
  }

  if (fromHeader === TOKEN || fromCookie === TOKEN) return true;

  response.writeHead(404, { 'content-type': 'text/plain' });
  response.end('not found\n');
  return false;
}

/** One browser request becomes one `lambda:Invoke` with an HTTP-shaped payload. */
async function forward(request, response) {
  const url = new URL(request.url, 'http://localhost');
  const path = url.pathname.replace(/^\/api/, '');
  // The gate's own parameter is not part of the management call.
  url.searchParams.delete('t');
  const body = await readBody(request);

  const event = {
    version: '2.0',
    routeKey: '$default',
    rawPath: path,
    rawQueryString: url.search.replace(/^\?/, ''),
    headers: { 'content-type': 'application/json' },
    queryStringParameters: Object.fromEntries(url.searchParams.entries()),
    requestContext: {
      http: { method: request.method, path },
    },
    body: body || undefined,
    isBase64Encoded: false,
  };

  const result = await lambda.send(
    new InvokeCommand({
      FunctionName: outputs.function_name,
      // Synchronous, because the caller wants the management API's answer. A schedule *fire* is
      // the asynchronous one, and that difference is the whole reason the DLQ wiring matters.
      InvocationType: 'RequestResponse',
      Payload: Buffer.from(JSON.stringify(event)),
    })
  );

  const raw = Buffer.from(result.Payload ?? []).toString('utf8');

  if (result.FunctionError) {
    return send(response, 502, { error: 'function error', detail: safe(raw) });
  }

  const answer = safe(raw);
  response.writeHead(answer?.statusCode ?? 200, {
    'content-type': 'application/json',
  });
  response.end(answer?.body ?? raw);
}

function readBody(request) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    request.on('data', (chunk) => chunks.push(chunk));
    request.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    request.on('error', reject);
  });
}

/**
 * Where the console gets the stack's identity.
 *
 * `terraform output` first, because on the machine that applied the stack it is always right. But
 * the state file is local and deliberately not in the repo, so a second checkout has no state and
 * would be stuck — hence the environment fallback. Everything the console needs is a function name
 * and a region; the function itself holds every other setting.
 */
function readTerraformOutputs() {
  const functionName = process.env.POC_FUNCTION_NAME;
  const region = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION;

  /**
   * The environment wins when it names a function, and the order matters.
   *
   * Terraform output is the convenient default on the machine that applied the stack, but it
   * answers for whichever workspace happens to be selected — so with two stacks live it cannot be
   * used to point one console at one of them. Explicit beats inferred: set POC_FUNCTION_NAME and
   * this console serves that function whatever the state directory says.
   */
  if (functionName && region) {
    return {
      function_name: functionName,
      region,
      account_id: process.env.POC_ACCOUNT_ID ?? '(from env)',
      table_name: process.env.POC_TABLE ?? '(from env)',
      schedule_group: process.env.POC_SCHEDULE_GROUP ?? '(from env)',
    };
  }

  const fromTerraform = tryTerraformOutputs();
  if (fromTerraform) return fromTerraform;

  console.error(
    'No stack outputs and no environment fallback.\n\n' +
      'On the machine that applied the stack:\n' +
      '  cd infra && terraform apply -var expected_account_id=<account>\n\n' +
      'Or point this console at a deployed function directly, which also overrides\n' +
      'whichever Terraform workspace is selected:\n' +
      '  POC_FUNCTION_NAME=poc-schedule AWS_REGION=eu-central-1 npm run console\n'
  );
  process.exit(1);
}

function tryTerraformOutputs() {
  try {
    const json = execFileSync('terraform', ['output', '-json'], {
      cwd: join(here, 'infra'),
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const parsed = JSON.parse(json);
    const flat = Object.fromEntries(
      Object.entries(parsed).map(([key, value]) => [key, value.value])
    );
    return flat.function_name ? flat : undefined;
  } catch {
    return undefined;
  }
}

function safe(text) {
  try {
    return JSON.parse(text);
  } catch {
    return undefined;
  }
}

function send(response, statusCode, payload) {
  response.writeHead(statusCode, { 'content-type': 'application/json' });
  response.end(JSON.stringify(payload));
}
