#!/usr/bin/env node
//
// The queen-sqs Node matrix suite: `@aws-sdk/client-sqs`, `@aws-sdk/client-sns`
// and sqs-consumer against a live facade, a live broker and a live Postgres.
//
//   protocols/queen-sqs/compat/rig.sh up
//   source protocols/queen-sqs/compat/.rig/env.sh
//   node protocols/queen-sqs/compat/js/run.mjs all
//
// Lanes (any combination; `all` is the default):
//
//   vectors    the MD5 algorithms against the goldens in protocols/queen-sqs/src/md5.rs
//   probe      what the installed SDK does about protocols, MD5s and errors,
//              proved against an in-process stub
//   sqs        the SQS inventory, mirroring compat/smoke_m0.py
//   sns        the SNS inventory, mirroring compat/smoke_m4_sns.py — and the
//              Query/XML codec, which the SQS client never speaks
//   consumer   sqs-consumer worker loops: handleMessage with throws and
//              redelivery, handleMessageBatch with partial acks
//
// `vectors` and `probe` need NOTHING running. The other three need the rig, and
// the run stops with `FAIL rig.reachable` rather than emitting a hundred
// connection errors if it is not there.
//
// The contract, from queen-kafka's CLIENT_MATRIX.md: the stack comes from the
// environment, one `ok`/`FAIL` line per assertion, a `RESULT:` line last, a
// nonzero exit when anything failed, and the protocol each client ACTUALLY
// spoke reported from the client's own requests.

import { ListQueuesCommand } from "@aws-sdk/client-sqs";

import { fail, finish, note } from "./lib/report.mjs";
import { teardown } from "./lib/queue.mjs";
import { packageVersion } from "./lib/versions.mjs";
import {
  ACCOUNT,
  ENDPOINT,
  PARTITIONS,
  REGION,
  RUN,
  makeSns,
  makeSqs,
  protocolLines,
} from "./lib/stack.mjs";

const LANES = ["vectors", "probe", "sqs", "sns", "consumer"];
const NEEDS_STACK = new Set(["sqs", "sns", "consumer"]);

function chosenLanes(argv) {
  const asked = argv.filter((a) => !a.startsWith("-"));
  if (asked.length === 0 || asked.includes("all")) return LANES;
  const unknown = asked.filter((a) => !LANES.includes(a));
  if (unknown.length) {
    console.error(`run.mjs: unknown lane(s) ${unknown.join(", ")}. It is one of: ${LANES.join(", ")}, all.`);
    process.exit(2);
  }
  return LANES.filter((lane) => asked.includes(lane));
}

/** One test, with its own guard: a blow-up costs its own assertions, not the run's. */
async function runTest(fn, ctx) {
  try {
    await fn(ctx);
  } catch (err) {
    fail(fn.name || "anonymous test", `unexpected exception: ${err?.name}: ${err?.message}`);
    for (const line of String(err?.stack ?? "").split("\n").slice(0, 12)) note(`  ${line.trim()}`);
  }
}

async function main() {
  const lanes = chosenLanes(process.argv.slice(2));

  note(`endpoint ${ENDPOINT}  region ${REGION}  account ${ACCOUNT}  partitions ${PARTITIONS}  run ${RUN}`);
  note(
    `node ${process.version}  @aws-sdk/client-sqs ${packageVersion("@aws-sdk/client-sqs")}  ` +
      `@aws-sdk/client-sns ${packageVersion("@aws-sdk/client-sns")}  sqs-consumer ${packageVersion("sqs-consumer")}`,
  );
  note(`lanes: ${lanes.join(", ")}`);

  const sqs = makeSqs();
  const sns = makeSns();
  const ctx = { sqs, sns };
  const needsStack = lanes.some((lane) => NEEDS_STACK.has(lane));

  if (needsStack) {
    try {
      await sqs.send(new ListQueuesCommand({}));
    } catch (err) {
      fail("rig.reachable", `${err?.name}: ${err?.message} (is protocols/queen-sqs/compat/rig.sh up?)`);
      console.log("RESULT: FAIL");
      return 1;
    }
  }

  let sawSns = false;
  for (const lane of lanes) {
    switch (lane) {
      case "vectors": {
        const { run } = await import("./scenarios/vectors.mjs");
        await run();
        break;
      }
      case "probe": {
        const { run } = await import("./scenarios/probe.mjs");
        await run();
        break;
      }
      case "sqs": {
        const { tests } = await import("./scenarios/sqs.mjs");
        for (const test of tests) await runTest(test, ctx);
        break;
      }
      case "sns": {
        const { tests } = await import("./scenarios/sns.mjs");
        sawSns = true;
        for (const test of tests) await runTest(test, ctx);
        break;
      }
      case "consumer": {
        const { tests } = await import("./scenarios/consumer.mjs");
        for (const test of tests) await runTest(test, ctx);
        break;
      }
      default:
        break;
    }
  }

  if (needsStack) {
    try {
      await teardown(sqs);
      if (sawSns) {
        const { teardownTopics } = await import("./scenarios/sns.mjs");
        await teardownTopics(sns);
      }
    } catch (err) {
      note(`teardown did not finish cleanly: ${err?.name}: ${err?.message}`);
    }
  }

  // The contract's protocol lines: what these clients ACTUALLY put on the wire,
  // counted per request, never inferred from a version number.
  for (const line of protocolLines()) note(line);

  return finish();
}

process.exitCode = await main();
