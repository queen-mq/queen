// What the installed SDK does, proved against a stub — no rig, no facade.
//
// The suite makes three claims about `@aws-sdk/client-sqs` and
// `@aws-sdk/client-sns` that a reader is entitled to see evidence for, and all
// three are claims about the CLIENT, not about queen-sqs:
//
//   1. which wire protocol each client speaks (JSON 1.0 for SQS, Query/XML for
//      SNS) — the suite contract wants this read from the client's own
//      behaviour and never assumed from a version number;
//   2. which MD5 fields this major validates client-side, and which it hands
//      back unchecked — the reason `lib/md5.mjs` exists;
//   3. how a facade's error rendering becomes a JS exception: which class, and
//      which of `name` / `Code` / `Type` carry which of SQS's two spellings.
//
// All three are answered here by pointing the real clients at a stub HTTP
// server on an ephemeral loopback port and feeding them responses this file
// controls — including deliberately WRONG checksums, which is the only way to
// see a validator that is working. It needs nothing running and takes about a
// second, so it is part of `all`: if the SDK's behaviour changes under us, the
// run says so in the same breath as everything else.

import { createServer } from "node:http";
import { createHash } from "node:crypto";

import {
  SQSClient,
  SendMessageCommand,
  ReceiveMessageCommand,
  QueueDoesNotExist,
} from "@aws-sdk/client-sqs";
import { SNSClient, CreateTopicCommand, NotFoundException } from "@aws-sdk/client-sns";

import { check, checkEq, note, fail } from "../lib/report.mjs";
import { attributesMd5, bodyMd5 } from "../lib/md5.mjs";
import { inspectSdkMd5, sdkMd5Summary } from "../lib/sdk-md5.mjs";

const md5 = (text) => createHash("md5").update(text, "utf8").digest("hex");

/** The stub: it answers whatever the current `plan` says, and remembers the request. */
async function withStub(fn) {
  const seen = [];
  let plan = () => ({ status: 200, headers: {}, body: "{}" });

  const server = createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      const request = {
        method: req.method,
        url: req.url,
        headers: req.headers,
        body: Buffer.concat(chunks).toString("utf8"),
      };
      seen.push(request);
      const answer = plan(request);
      res.writeHead(answer.status, {
        "content-type": answer.contentType ?? "application/x-amz-json-1.0",
        ...(answer.headers ?? {}),
      });
      res.end(answer.body);
    });
  });

  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  try {
    return await fn({
      endpoint: `http://127.0.0.1:${port}`,
      seen,
      answerWith: (fn2) => {
        plan = fn2;
      },
    });
  } finally {
    server.close();
  }
}

function client(endpoint) {
  return new SQSClient({
    endpoint,
    region: "queen-1",
    credentials: { accessKeyId: "PROBE", secretAccessKey: "probe" },
    maxAttempts: 1,
  });
}

export async function run() {
  // ----------------------------------------------------- what the source says
  const finding = inspectSdkMd5();
  note(sdkMd5Summary(finding));
  for (const line of finding.evidence) note(`  evidence: ${line}`);
  check(
    "SdkMd5.installed_major_was_inspected",
    finding.inspected,
    "neither @aws-sdk/client-sqs nor its checksum middleware could be read from node_modules",
  );

  await withStub(async ({ endpoint, seen, answerWith }) => {
    const sqs = client(endpoint);
    const queueUrl = `${endpoint}/000000000000/probe`;
    const body = "probe body";

    // ------------------------------------------------ 1. the protocol spoken
    answerWith(() => ({
      status: 200,
      body: JSON.stringify({ MessageId: "11111111-1111-1111-1111-111111111111", MD5OfMessageBody: md5(body) }),
    }));
    await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body }));
    const sent = seen.at(-1);
    checkEq("Protocol.sqs_content_type", sent.headers["content-type"], "application/x-amz-json-1.0");
    checkEq("Protocol.sqs_target", sent.headers["x-amz-target"], "AmazonSQS.SendMessage");
    check(
      "Protocol.sqs_body_is_json",
      sent.body.startsWith("{") && JSON.parse(sent.body).MessageBody === body,
      `got ${sent.body.slice(0, 120)}`,
    );
    check(
      "Protocol.sqs_is_sigv4_signed_for_sqs",
      /AWS4-HMAC-SHA256 Credential=PROBE\/\d{8}\/queen-1\/sqs\/aws4_request/.test(sent.headers.authorization ?? ""),
      `got ${sent.headers.authorization}`,
    );

    // ------------------------------- 2. what the SDK checks, and what it does not
    // A WRONG body digest. If this major validates, the send raises before the
    // caller ever sees the answer.
    answerWith(() => ({
      status: 200,
      body: JSON.stringify({ MessageId: "22222222-2222-2222-2222-222222222222", MD5OfMessageBody: md5("something else") }),
    }));
    let raised = null;
    try {
      await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body }));
    } catch (err) {
      raised = err;
    }
    checkEq("SdkMd5.send_body_validation_matches_the_source", raised !== null, finding.validates.bodyOnSend);
    if (raised) note(`send with a corrupt body digest raised: ${raised.message}`);

    // A wrong ATTRIBUTE digest with a correct body one. Every major to date
    // returns this to the caller unchecked — which is the whole reason this
    // suite computes the attribute digest itself.
    const attributes = { event: { DataType: "String", StringValue: "probe" } };
    answerWith(() => ({
      status: 200,
      body: JSON.stringify({
        MessageId: "33333333-3333-3333-3333-333333333333",
        MD5OfMessageBody: md5(body),
        MD5OfMessageAttributes: "00000000000000000000000000000000",
      }),
    }));
    let attributeRaise = null;
    let answer = null;
    try {
      answer = await sqs.send(
        new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body, MessageAttributes: attributes }),
      );
    } catch (err) {
      attributeRaise = err;
    }
    checkEq(
      "SdkMd5.attribute_validation_matches_the_source",
      attributeRaise !== null,
      finding.validates.attributes,
    );
    check(
      "SdkMd5.a_wrong_attribute_digest_reaches_the_caller_unchecked",
      attributeRaise === null && answer?.MD5OfMessageAttributes === "00000000000000000000000000000000",
      attributeRaise
        ? `the SDK raised: ${attributeRaise.message}`
        : `got ${answer?.MD5OfMessageAttributes}`,
    );
    // ...and this suite's own algorithm is the one that would have caught it.
    check(
      "SdkMd5.this_suite_would_have_caught_it",
      attributesMd5(attributes) !== "00000000000000000000000000000000",
      "lib/md5.mjs agreed with the corrupt digest",
    );

    // The receive side, per message.
    answerWith(() => ({
      status: 200,
      body: JSON.stringify({
        Messages: [
          {
            MessageId: "44444444-4444-4444-4444-444444444444",
            ReceiptHandle: "probe-handle",
            Body: body,
            MD5OfBody: md5("not the body"),
          },
        ],
      }),
    }));
    let receiveRaise = null;
    try {
      await sqs.send(new ReceiveMessageCommand({ QueueUrl: queueUrl }));
    } catch (err) {
      receiveRaise = err;
    }
    checkEq(
      "SdkMd5.receive_body_validation_matches_the_source",
      receiveRaise !== null,
      finding.validates.bodyOnReceive,
    );
    check("SdkMd5.suite_body_digest_agrees_with_the_sdks", bodyMd5(body) === md5(body), "the two disagree");

    // ------------------------------------------- 3. how an SQS error arrives
    // The facade's JSON rendering, exactly: `__type` plus the compatibility
    // header carrying the LEGACY code and the fault.
    answerWith(() => ({
      status: 400,
      headers: { "x-amzn-query-error": "AWS.SimpleQueueService.NonExistentQueue;Sender" },
      body: JSON.stringify({
        __type: "com.amazonaws.sqs#QueueDoesNotExist",
        message: "The specified queue does not exist.",
      }),
    }));
    let sqsError = null;
    try {
      await sqs.send(new SendMessageCommand({ QueueUrl: queueUrl, MessageBody: body }));
    } catch (err) {
      sqsError = err;
    }
    if (!sqsError) {
      fail("Errors.sqs_json_rendering_raises", "the call succeeded on a 400");
    } else {
      checkEq("Errors.sqs_exception_class", sqsError.constructor?.name, "QueueDoesNotExist");
      check("Errors.sqs_is_the_modelled_class", sqsError instanceof QueueDoesNotExist, `got ${sqsError.name}`);
      checkEq("Errors.sqs_name_is_the_shape", sqsError.name, "QueueDoesNotExist");
      checkEq("Errors.sqs_Code_is_the_legacy_code", sqsError.Code, "AWS.SimpleQueueService.NonExistentQueue");
      checkEq("Errors.sqs_Type_is_the_fault", sqsError.Type, "Sender");
      checkEq("Errors.sqs_status", sqsError.$metadata?.httpStatusCode, 400);
    }

    // ------------------------------------------ 4. SNS: Query in, XML out
    const sns = new SNSClient({
      endpoint,
      region: "queen-1",
      credentials: { accessKeyId: "PROBE", secretAccessKey: "probe" },
      maxAttempts: 1,
    });
    answerWith(() => ({
      status: 200,
      contentType: "text/xml",
      body:
        '<CreateTopicResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/">' +
        "<CreateTopicResult><TopicArn>arn:aws:sns:queen-1:000000000000:probe</TopicArn></CreateTopicResult>" +
        "<ResponseMetadata><RequestId>probe</RequestId></ResponseMetadata></CreateTopicResponse>",
    }));
    const created = await sns.send(new CreateTopicCommand({ Name: "probe" }));
    const snsRequest = seen.at(-1);
    checkEq("Protocol.sns_content_type", snsRequest.headers["content-type"], "application/x-www-form-urlencoded");
    check(
      "Protocol.sns_has_no_json_target",
      snsRequest.headers["x-amz-target"] === undefined,
      `got ${snsRequest.headers["x-amz-target"]}`,
    );
    check(
      "Protocol.sns_body_is_the_query_form",
      snsRequest.body.startsWith("Action=CreateTopic&Version=2010-03-31"),
      `got ${snsRequest.body.slice(0, 120)}`,
    );
    check(
      "Protocol.sns_is_sigv4_signed_for_sns",
      /AWS4-HMAC-SHA256 Credential=PROBE\/\d{8}\/queen-1\/sns\/aws4_request/.test(
        snsRequest.headers.authorization ?? "",
      ),
      `got ${snsRequest.headers.authorization}`,
    );
    checkEq("Protocol.sns_xml_is_parsed", created.TopicArn, "arn:aws:sns:queen-1:000000000000:probe");

    // ------------------------------------------ 5. how an SNS error arrives
    answerWith(() => ({
      status: 404,
      contentType: "text/xml",
      body:
        '<ErrorResponse xmlns="http://sns.amazonaws.com/doc/2010-03-31/"><Error>' +
        "<Type>Sender</Type><Code>NotFound</Code><Message>Topic does not exist</Message>" +
        "</Error><RequestId>probe</RequestId></ErrorResponse>",
    }));
    let snsError = null;
    try {
      await sns.send(new CreateTopicCommand({ Name: "probe" }));
    } catch (err) {
      snsError = err;
    }
    if (!snsError) {
      fail("Errors.sns_xml_rendering_raises", "the call succeeded on a 404");
    } else {
      checkEq("Errors.sns_exception_class", snsError.constructor?.name, "NotFoundException");
      check("Errors.sns_is_the_modelled_class", snsError instanceof NotFoundException, `got ${snsError.name}`);
      checkEq("Errors.sns_Code_is_the_shapes_own_code", snsError.Code, "NotFound");
      checkEq("Errors.sns_Type_is_the_fault", snsError.Type, "Sender");
      checkEq("Errors.sns_status", snsError.$metadata?.httpStatusCode, 404);
    }
  });
}
