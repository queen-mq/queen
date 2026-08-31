// The stack, from the environment — never from a hardcoded address — and the
// two clients pointed at it.
//
// `protocols/queen-sqs/compat/rig.sh up` writes `.rig/env.sh`; a person sources it and
// runs this suite. The defaults below are the rig's own so that a bare
// `node run.mjs all` after an `up` does the obvious thing, and the endpoint is
// printed on the run's first line so no reader has to guess which stack the
// numbers came from.
//
// WHICH PROTOCOL THE CLIENT ACTUALLY SPOKE is also this file's job, and it is
// part of the suite contract ("each rig reports which protocol its client
// actually spoke, read from the client's own debug stream, never assumed"). It
// is recorded off the HttpRequest the SDK itself built, in a middleware sitting
// at the very end of `finalizeRequest` — after signing, so the record also
// carries the SigV4 credential scope and can say which SERVICE each half of the
// run signed for. Two SDK clients against ONE listener is the interesting shape
// here: `@aws-sdk/client-sqs` speaks AWS JSON 1.0 and `@aws-sdk/client-sns`
// speaks Query/XML, so a single `node run.mjs all` crosses both codecs.

import { SQSClient } from "@aws-sdk/client-sqs";
import { SNSClient } from "@aws-sdk/client-sns";
import { randomUUID } from "node:crypto";

// A trailing slash would end up in every URL this suite compares against the
// facade's own, which mints them without one.
export const ENDPOINT = (process.env.QUEEN_SQS_ENDPOINT ?? "http://127.0.0.1:19324").replace(/\/+$/, "");
export const REGION = process.env.QUEEN_SQS_REGION ?? "queen-1";
export const ACCOUNT = process.env.QUEEN_SQS_ACCOUNT ?? "000000000000";
export const PARTITIONS = Number(process.env.QUEEN_SQS_PARTITIONS ?? "8");
export const AKID = process.env.AWS_ACCESS_KEY_ID ?? "QSQSTEST";
export const SECRET = process.env.AWS_SECRET_ACCESS_KEY ?? "qsqssecret";

/** Every name this run creates carries it, so two runs never collide. */
export const RUN = randomUUID().slice(0, 8);

// ------------------------------------------------------------ protocol record

/** service → { label → { count, actions:Set, signedFor:Set } } */
const spoken = new Map();

function headerOf(request, name) {
  const headers = request?.headers ?? {};
  for (const key of Object.keys(headers)) {
    if (key.toLowerCase() === name) return headers[key];
  }
  return undefined;
}

/** The service the SigV4 credential scope names: `.../<region>/<service>/aws4_request`. */
function signedService(request) {
  const auth = headerOf(request, "authorization") ?? "";
  const scope = /Credential=[^/]+\/[^/]+\/[^/]+\/([^/]+)\/aws4_request/.exec(auth);
  return scope ? scope[1] : "unsigned";
}

function bodyText(request) {
  const body = request?.body;
  return typeof body === "string" ? body : "";
}

/**
 * The two shapes, told apart by the request itself and not by the SDK's
 * version: AWS JSON 1.0 carries `X-Amz-Target: <Service>.<Action>` over an
 * `application/x-amz-json-1.0` body; Query carries neither and a form-encoded
 * `Action=...&Version=...` body.
 */
function classify(request) {
  const target = headerOf(request, "x-amz-target");
  const contentType = headerOf(request, "content-type") ?? "";
  if (target && contentType.includes("json")) {
    const prefix = String(target).split(".")[0];
    return { label: `AWS JSON 1.0 (${contentType}; X-Amz-Target: ${prefix}.*)`, action: String(target).split(".")[1] };
  }
  if (contentType.includes("x-www-form-urlencoded")) {
    const body = bodyText(request);
    const action = /(?:^|&)Action=([^&]*)/.exec(body)?.[1] ?? "?";
    const version = /(?:^|&)Version=([^&]*)/.exec(body)?.[1] ?? "?";
    return { label: `Query/XML (${contentType}; Version=${version})`, action };
  }
  return {
    label: `unrecognized (Content-Type: ${contentType || "none"}, X-Amz-Target: ${target ?? "none"})`,
    action: "?",
  };
}

function record(service, request) {
  const { label, action } = classify(request);
  const perService = spoken.get(service) ?? new Map();
  const entry = perService.get(label) ?? { count: 0, actions: new Set(), signedFor: new Set() };
  entry.count += 1;
  entry.actions.add(action);
  entry.signedFor.add(signedService(request));
  perService.set(label, entry);
  spoken.set(service, perService);
}

function recorder(service) {
  return {
    applyToStack: (stack) => {
      stack.add(
        (next) => async (args) => {
          if (args.request && args.request.headers) record(service, args.request);
          return next(args);
        },
        // Last thing before the request handler: `priority: "low"` puts it after
        // the signer, which is what lets `signedFor` be read off Authorization.
        { step: "finalizeRequest", priority: "low", name: "queenRecordProtocol", override: true },
      );
    },
  };
}

/** What was spoken, per service: `[{service, label, count, signedFor, actions}]`. */
export function protocolsSpoken() {
  const out = [];
  for (const [service, labels] of spoken) {
    for (const [label, entry] of labels) {
      out.push({
        service,
        label,
        count: entry.count,
        signedFor: [...entry.signedFor].sort(),
        actions: [...entry.actions].sort(),
      });
    }
  }
  return out.sort((a, b) => b.count - a.count);
}

/** The `#` lines the contract asks for, one per (service, protocol) pair. */
export function protocolLines() {
  return protocolsSpoken().map(
    (p) =>
      `protocol spoken (${p.service}): ${p.label} — ${p.count} request(s), ` +
      `signed for ${p.signedFor.join("+")}`,
  );
}

// ------------------------------------------------------------------- clients

function credentials(secret) {
  return { accessKeyId: AKID, secretAccessKey: secret ?? SECRET };
}

export function makeSqs({ secret, service = "sqs" } = {}) {
  const client = new SQSClient({
    endpoint: ENDPOINT,
    region: REGION,
    credentials: credentials(secret),
    // The rig is one process on loopback; a retry would turn a facade refusal
    // into three of them in the log and tell the reader nothing.
    maxAttempts: 1,
  });
  client.middlewareStack.use(recorder(service));
  return client;
}

export function makeSns({ secret } = {}) {
  const client = new SNSClient({
    endpoint: ENDPOINT,
    region: REGION,
    credentials: credentials(secret),
    maxAttempts: 1,
  });
  client.middlewareStack.use(recorder("sns"));
  return client;
}

// ------------------------------------------------------------------- helpers

export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

export function isUuid(value) {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value);
}

/** A plausible epoch-millisecond stamp: 13 digits, this decade, not seconds. */
export function looksLikeEpochMillis(value) {
  if (typeof value !== "string" || !/^\d{13}$/.test(value)) return false;
  const ms = Number(value);
  return ms > 1_600_000_000_000 && ms < 4_000_000_000_000;
}

export function queueArn(name) {
  return `arn:aws:sqs:${REGION}:${ACCOUNT}:${name}`;
}

export function topicArn(name) {
  return `arn:aws:sns:${REGION}:${ACCOUNT}:${name}`;
}
