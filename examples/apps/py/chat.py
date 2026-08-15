# docs:start(app-py-chat)
#
# A chat messaging system.
#
# This is the application Queen was written for. A hotel messaging product ran
# on Kafka and kept stalling: some conversations need a translation or an agent
# reply before the next message can be handled, and on a shared partition one
# slow conversation holds up every conversation behind it.
#
# The fix is structural rather than operational: one ordered lane per
# conversation, created by the first message sent to it. A conversation that
# takes ten seconds delays itself and nothing else.
#
# What this program builds:
#
#   chat-messages (one partition per conversation)
#     |-- group "delivery"    fast, marks each message as delivered
#     `-- group "enrichment"  slow on conversations that need translation
#
# And what it proves: every message reaches both groups exactly once, in the
# order it was sent inside its own conversation, and the conversations that
# need no translation finish while the slow one is still working.
#
# Run it:
#   QUEEN_URL=http://localhost:6632 python3 chat.py

import asyncio
import os
import sys
import time

from queen import Queen

QUEEN_URL = os.environ.get("QUEEN_URL", "http://localhost:6632")

# The name is prefixed per language and suffixed per run, so every application
# in every language can share one broker and no run inherits state from another.
RUN = f"{int(time.time() * 1000):x}"
MESSAGES = f"app-py-chat-{RUN}"

# Three conversations. The one in Japanese needs a translation pass, which is
# the slow work: 400 ms a message against 10 ms for the rest.
CONVERSATIONS = {
    "conv-en-1": {"locale": "en", "needs_translation": False},
    "conv-en-2": {"locale": "en", "needs_translation": False},
    "conv-jp-1": {"locale": "jp", "needs_translation": True},
}
MESSAGES_PER_CONVERSATION = 6

CHECKS = 0


def check(condition: bool, description: str) -> None:
    """Record one verified fact, or abort the run.

    This raises instead of using the `assert` statement, because `python3 -O`
    removes `assert` and the checks are the whole point of the program.
    """
    global CHECKS
    if not condition:
        raise AssertionError(description)
    CHECKS += 1
    print(f"  ok: {description}")


async def main() -> int:
    # The whole client is async: every call below is awaited, and this is the
    # one event loop they all run on. Unlike the JavaScript client there is no
    # handleSignals switch, so SIGINT and SIGTERM are always handled for you;
    # the orderly shutdown of a run that ends normally is close(), at the bottom.
    queen = Queen(url=QUEEN_URL)
    verdict, failed = "", False

    try:
        print(f"broker {QUEEN_URL}")

        # Leases are what make a crashed worker safe: a message whose handler
        # dies is redelivered once the lease expires. retry_limit bounds how
        # many times that can happen before the message is dead-lettered
        # instead. The config keys are snake_case in Python and the client
        # converts them to the camelCase the broker expects.
        await queen.queue(MESSAGES).config({"lease_time": 60, "retry_limit": 3}).create()

        # ---------------------------------------------------------- producing
        #
        # A chat client sends a message: one push, into the partition named
        # after the conversation. Nothing was declared for this conversation in
        # advance, and nothing has to be cleaned up when it goes quiet.
        print("\nsending")
        sent = []
        for seq in range(1, MESSAGES_PER_CONVERSATION + 1):
            for conversation_id, meta in CONVERSATIONS.items():
                message = {
                    "conversationId": conversation_id,
                    "seq": seq,
                    "locale": meta["locale"],
                    "body": f"message {seq} in {conversation_id}",
                    "sentAt": int(time.time() * 1000),
                }
                # The transaction id is the client's own idempotency key: a
                # retry of this send, from a phone on a flaky network, writes
                # nothing the second time and answers with the first message's
                # id. The item key stays camelCase here, because it is the wire
                # name rather than a client option.
                await queen.queue(MESSAGES).partition(conversation_id).push(
                    {"transactionId": f"{conversation_id}-{seq}", "data": message}
                )
                sent.append(message)
        print(f"  {len(sent)} messages across {len(CONVERSATIONS)} conversations")

        # A resend of the same message: the client retried because it never saw
        # the first answer. The broker recognises the transaction id and stores
        # nothing. What comes back is the broker's own reply, one entry per
        # item, with the broker's own key names.
        results = await queen.queue(MESSAGES).partition("conv-en-1").push(
            {
                "transactionId": "conv-en-1-1",
                "data": {"conversationId": "conv-en-1", "seq": 1, "body": "resent by the phone"},
            }
        )
        check(
            results[0]["status"] == "duplicate",
            "a resent message was deduplicated, not stored twice",
        )

        # --------------------------------------------------------- delivering
        #
        # The delivery worker is what marks a message as delivered to the
        # recipients. It is fast and must never fall behind, which is why it is
        # its own consumer group: it shares no cursor with the slow work below.
        #
        # concurrency(3) runs three poll loops, and each pop claims a partition,
        # so the three conversations are drained in parallel by three workers.
        # The handler is an async def taking one message: consume() awaits it
        # for every message and acknowledges on return.
        print("\ndelivering")
        delivered: dict = {}

        async def deliver(msg) -> None:
            await asyncio.sleep(0.01)
            delivered.setdefault(msg["data"]["conversationId"], []).append(msg["data"]["seq"])

        await (
            queen.queue(MESSAGES)
            .group("delivery")
            # A group created after the messages were pushed starts at the tail,
            # so without this it would see nothing.
            .subscription_mode("all")
            .concurrency(3)
            .each()
            .limit(len(sent))
            # Stop after 10s of silence, so a lost message fails the run instead
            # of hanging it.
            .idle_millis(10000)
            .consume(deliver)
        )

        check(
            sum(len(seqs) for seqs in delivered.values()) == len(sent),
            "delivery saw every message exactly once",
        )
        for conversation_id, seqs in delivered.items():
            check(seqs == sorted(seqs), f"{conversation_id} was delivered in order")

        # --------------------------------------------------------- enrichment
        #
        # The slow group. It reads the same messages through its own cursor, and
        # the Japanese conversation costs 400 ms a message because it has to be
        # translated before it can be answered.
        #
        # This is where a shared partition would hurt: on a hashed topic these
        # messages would sit in the same lane as the English ones and hold them
        # up. Here each conversation has its own lane, so the English
        # conversations finish while the Japanese one is still being translated.
        # The timings below are the proof.
        print("\nenriching")
        finished_at: dict = {}
        # monotonic() rather than time(): these are durations, and a clock that
        # steps sideways mid-run must not turn a real ordering into a fake one.
        started = time.monotonic()

        async def enrich(msg) -> None:
            meta = CONVERSATIONS[msg["data"]["conversationId"]]
            await asyncio.sleep(0.4 if meta["needs_translation"] else 0.01)
            finished_at[msg["data"]["conversationId"]] = int((time.monotonic() - started) * 1000)

        await (
            queen.queue(MESSAGES)
            .group("enrichment")
            .subscription_mode("all")
            .concurrency(3)
            .each()
            .limit(len(sent))
            .idle_millis(15000)
            .consume(enrich)
        )

        slow = finished_at["conv-jp-1"]
        fast = max(finished_at["conv-en-1"], finished_at["conv-en-2"])
        print(f"  english done after {fast} ms, japanese after {slow} ms")

        check(
            fast < slow,
            "the conversations needing no translation finished first, in the same worker pool",
        )
        check(
            slow > MESSAGES_PER_CONVERSATION * 300,
            "the slow conversation really was slow, so the comparison means something",
        )

        # -------------------------------------------------------------- replay
        #
        # A new feature needs the history: sentiment scoring over everything ever
        # said. It is a new consumer group reading from the beginning, and it
        # costs no producer change and no second copy of the data.
        print("\nbackfilling a new consumer")
        scored = 0

        async def score(msg) -> None:
            nonlocal scored
            scored += 1

        await (
            queen.queue(MESSAGES)
            .group("sentiment")
            .subscription_mode("all")
            .concurrency(3)
            .each()
            .limit(len(sent))
            .idle_millis(10000)
            .consume(score)
        )

        check(scored == len(sent), "a group added today read the whole history")

        # Clean up on success only: a failed run leaves the queue on the broker
        # to be looked at.
        await queen.queue(MESSAGES).delete()

        verdict = f"\nPASS: {CHECKS} checks"
    except Exception as err:
        verdict, failed = f"\nFAIL: {err}", True
    finally:
        # close() flushes the client-side buffers and closes the HTTP pool. It
        # narrates its own shutdown on stdout, which is why the verdict is
        # printed after it rather than before: PASS or FAIL stays the last line
        # of a run.
        await queen.close()

    # A failure goes to stderr, like the rest of the set. Flush stdout first so
    # the verdict still lands last when the two are piped into one file.
    sys.stdout.flush()
    print(verdict, file=sys.stderr if failed else sys.stdout)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
# docs:end
