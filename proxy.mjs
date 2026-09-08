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
    if (request.url === '/' || request.url === '/index.html') {
      const page = await readFile(join(here, 'ui', 'index.html'));
      response.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
      response.end(page);
      return;
    }

    if (request.url === '/config') {
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

/** One browser request becomes one `lambda:Invoke` with an HTTP-shaped payload. */
async function forward(request, response) {
  const url = new URL(request.url, 'http://localhost');
  const path = url.pathname.replace(/^\/api/, '');
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
  const fromTerraform = tryTerraformOutputs();
  if (fromTerraform) return fromTerraform;

  const functionName = process.env.POC_FUNCTION_NAME;
  const region = process.env.AWS_REGION ?? process.env.AWS_DEFAULT_REGION;

  if (functionName && region) {
    return {
      function_name: functionName,
      region,
      account_id: process.env.POC_ACCOUNT_ID ?? '(unknown)',
      table_name: process.env.POC_TABLE ?? '(unknown)',
      schedule_group: process.env.POC_SCHEDULE_GROUP ?? '(unknown)',
    };
  }

  console.error(
    'No stack outputs and no environment fallback.\n\n' +
      'On the machine that applied the stack:\n' +
      '  cd infra && terraform apply -var expected_account_id=<account>\n\n' +
      'On any other machine, point the console at the already-deployed function:\n' +
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
