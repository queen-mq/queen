<?php

namespace Queen\Tests;

use GuzzleHttp\HandlerStack;
use GuzzleHttp\Promise\FulfilledPromise;
use GuzzleHttp\Promise\PromiseInterface;
use GuzzleHttp\Psr7\Response;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhpExtension;
use PHPUnit\Framework\TestCase;
use Psr\Http\Message\RequestInterface;
use Queen\Exceptions\ConflationUnsupportedException;
use Queen\Http\PopFrames;
use Queen\Http\PushBinary;
use Queen\Http\Zstd;
use Queen\Queen;

/**
 * The binary push body (Http\PushBinary) and the framed pop answer
 * (Http\PopFrames), asserted against a scripted handler.
 *
 * What is at stake: a client that sets none of the three options must send
 * the JSON requests every released broker knows, and a consumer that sets
 * framedPop must be handed exactly the messages the JSON answer gives it.
 *
 * pushCompression and framedPop need the zstd extension; their tests are
 * skipped where it is not loaded.
 */
class WireFormsTest extends TestCase
{
    private const TXN = '0190aaaa-0000-7000-8000-000000000001';
    private const TRACE = '0190aaaa-0000-4000-8000-000000000001';

    // The JSON answer of two messages of partition 7 ("p7") of "orders", as
    // the broker renders it.
    private const TWO_MESSAGES_JSON = '{"success":true,"queue":"orders","partition":"p7","partitionId":"7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","messages":['
        . '{"id":"01000000-0000-7000-8000-000000000000","transactionId":"t-10","traceId":null,"data":{"n":10},"producerSub":null,"createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":10},'
        . '{"id":"02000000-0000-7000-8000-000000000000","transactionId":"t-11","traceId":null,"data":{"n":11,"s":"x"},"producerSub":"svc","createdAt":"2023-11-14T22:13:20.000Z","partitionId":"7","partition":"p7","leaseId":"lease-1","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":3,"offset":11}'
        . '],"partitionsClaimed":1}';

    /** @var list<RequestInterface> */
    private array $requests = [];

    /**
     * A client whose every request gets $answer($request): a
     * [status, content type, body] triple.
     */
    private function queen(\Closure $answer, array $config = []): Queen
    {
        $this->requests = [];
        $handler = function (RequestInterface $request, array $options) use ($answer): PromiseInterface {
            $this->requests[] = $request;
            [$status, $contentType, $body] = $answer($request);

            return new FulfilledPromise(new Response($status, ['Content-Type' => $contentType], $body));
        };

        return new Queen(['url' => 'http://wire.local', 'retryAttempts' => 1, 'handler' => HandlerStack::create($handler)] + $config);
    }

    private static function queued(int $count = 1): \Closure
    {
        return static fn (): array => [200, 'application/json', json_encode(array_fill(0, $count, ['status' => 'queued']))];
    }

    private static function json(string $body = self::TWO_MESSAGES_JSON): \Closure
    {
        return static fn (): array => [200, 'application/json', $body];
    }

    private static function framed(string $body): \Closure
    {
        return static fn (): array => [200, PopFrames::CONTENT_TYPE, $body];
    }

    private static function nothing(): \Closure
    {
        return static fn (): array => [204, 'application/json', ''];
    }

    /** @return list<RequestInterface> */
    private function pushes(): array
    {
        return array_values(array_filter(
            $this->requests,
            static fn (RequestInterface $r): bool => $r->getMethod() === 'POST' && $r->getUri()->getPath() === '/api/v1/push',
        ));
    }

    /** @return list<RequestInterface> */
    private function pops(): array
    {
        return array_values(array_filter(
            $this->requests,
            static fn (RequestInterface $r): bool => $r->getMethod() === 'GET' && str_starts_with($r->getUri()->getPath(), '/api/v1/pop'),
        ));
    }

    private static function param(RequestInterface $request, string $name): ?string
    {
        parse_str($request->getUri()->getQuery(), $query);

        return $query[$name] ?? null;
    }

    // ===========================
    // Reading a binary push body
    // ===========================

    private static function u16(string $bytes, int &$at): int
    {
        $at += 2;

        return unpack('v', substr($bytes, $at - 2, 2))[1];
    }

    private static function u32(string $bytes, int &$at): int
    {
        $at += 4;

        return unpack('V', substr($bytes, $at - 4, 4))[1];
    }

    private static function take(string $bytes, int &$at, int $length): string
    {
        $at += $length;

        return substr($bytes, $at - $length, $length);
    }

    /** The groups of a plain binary push body (after magic and flags). */
    private static function plainGroups(string $bytes, bool $withTxn): array
    {
        $at = 0;
        $groups = [];
        for ($g = self::u32($bytes, $at); $g > 0; $g--) {
            $group = [
                'queue' => self::take($bytes, $at, self::u16($bytes, $at)),
                'partition' => self::take($bytes, $at, self::u16($bytes, $at)),
                'items' => [],
            ];
            for ($n = self::u32($bytes, $at); $n > 0; $n--) {
                $txn = $withTxn ? self::take($bytes, $at, self::u16($bytes, $at)) : null;
                $group['items'][] = [$txn, self::take($bytes, $at, self::u32($bytes, $at))];
            }
            $groups[] = $group;
        }
        self::assertSame(strlen($bytes), $at, 'bytes left after the last payload');

        return $groups;
    }

    /** The groups of a framed binary push body, each with its frames unpacked. */
    private static function framedGroups(string $bytes): array
    {
        $at = 0;
        $groups = [];
        for ($g = self::u32($bytes, $at); $g > 0; $g--) {
            $group = [
                'queue' => self::take($bytes, $at, self::u16($bytes, $at)),
                'partition' => self::take($bytes, $at, self::u16($bytes, $at)),
                'count' => self::u32($bytes, $at),
                'frames' => [],
            ];
            $raw = Zstd::uncompress(self::take($bytes, $at, self::u32($bytes, $at)));
            $in = 0;
            while ($in < strlen($raw)) {
                $group['frames'][] = self::unframe($raw, $in);
            }
            $groups[] = $group;
        }
        self::assertSame(strlen($bytes), $at, 'bytes left after the last group');

        return $groups;
    }

    /**
     * One client-built frame at $at: u32 body_len | flags | id | txn
     * | [headers, flag 8] | payload.
     */
    private static function unframe(string $raw, int &$at): array
    {
        $frame = self::take($raw, $at, self::u32($raw, $at));
        $p = 0;
        $flags = ord(self::take($frame, $p, 1));
        $id = self::take($frame, $p, 16);
        $txn = self::take($frame, $p, self::u16($frame, $p));
        $headers = $flags & 8 ? self::take($frame, $p, self::u16($frame, $p)) : null;

        return ['flags' => $flags, 'id' => $id, 'txn' => $txn, 'headers' => $headers, 'payload' => substr($frame, $p)];
    }

    /** A headers block read back: u16 count | (u8 len, name | u16 len, value) x count. */
    private static function headersOf(string $block): array
    {
        $at = 0;
        $headers = [];
        for ($n = self::u16($block, $at); $n > 0; $n--) {
            $name = self::take($block, $at, ord(self::take($block, $at, 1)));
            $headers[$name] = self::take($block, $at, self::u16($block, $at));
        }
        self::assertSame(strlen($block), $at, 'bytes left after the last header');

        return $headers;
    }

    // ===========================
    // Push
    // ===========================

    public function testAPushIsJsonByDefault(): void
    {
        $queen = $this->queen(self::queued());

        $result = $queen->queue('orders')->partition('p1')->push(['data' => ['a' => 1]])->execute();

        $this->assertSame('queued', $result[0]['status']);
        $sent = $this->pushes()[0];
        $this->assertSame('application/json', $sent->getHeaderLine('Content-Type'));
        $body = json_decode((string) $sent->getBody(), true, 512, JSON_THROW_ON_ERROR);
        $this->assertSame(['items'], array_keys($body));
        $this->assertSame(['a' => 1], $body['items'][0]['payload']);
    }

    public function testBinaryPushSendsTheBinaryBodyWithEachTransactionId(): void
    {
        $queen = $this->queen(self::queued(), ['binaryPush' => true]);

        $result = $queen->queue('orders')->partition('p1')
            ->push(['data' => ['a' => 1], 'transactionId' => self::TXN])->execute();

        $this->assertSame('queued', $result[0]['status']);
        $sent = $this->pushes()[0];
        $this->assertSame('application/x-queen-push', $sent->getHeaderLine('Content-Type'));
        $bytes = (string) $sent->getBody();
        $this->assertSame("QPB1\x04", substr($bytes, 0, 5));
        $this->assertSame(
            [['queue' => 'orders', 'partition' => 'p1', 'items' => [[self::TXN, '{"a":1}']]]],
            self::plainGroups(substr($bytes, 5), true),
        );
    }

    public function testConsecutiveItemsOfOnePartitionShareAGroup(): void
    {
        $item = static fn (string $partition, int $payload): array => [
            'queue' => 'q', 'partition' => $partition, 'payload' => $payload, 'transactionId' => "t{$payload}",
        ];

        $bytes = PushBinary::body([$item('a', 1), $item('a', 2), $item('b', 3), $item('a', 4)], false);

        $this->assertSame("QPB1\x04", substr($bytes, 0, 5));
        $this->assertSame(
            [['a', [['t1', '1'], ['t2', '2']]], ['b', [['t3', '3']]], ['a', [['t4', '4']]]],
            array_map(static fn (array $g): array => [$g['partition'], $g['items']], self::plainGroups(substr($bytes, 5), true)),
        );
    }

    public function testBinaryPushCarriesPayloadsThatAreNotObjectsAndNonAsciiText(): void
    {
        $payloads = ["testo \"con\" virgolette\n", [1, 2], 7, null, ['è' => '€']];
        $queen = $this->queen(self::queued(5), ['binaryPush' => true]);

        $queen->queue('ordini')->partition('città')
            ->push(array_map(static fn (mixed $p): array => ['data' => $p], $payloads))->execute();

        [$group] = self::plainGroups(substr((string) $this->pushes()[0]->getBody(), 5), true);
        $this->assertSame('città', $group['partition']);
        $this->assertSame(
            $payloads,
            array_map(static fn (array $item): mixed => json_decode($item[1], true, 512, JSON_THROW_ON_ERROR), $group['items']),
        );
    }

    public function testAnItemWithATraceIdStaysJson(): void
    {
        $queen = $this->queen(self::queued(), ['binaryPush' => true]);

        $queen->queue('orders')->push(['data' => ['a' => 1], 'traceId' => self::TRACE])->execute();

        $sent = $this->pushes()[0];
        $this->assertSame('application/json', $sent->getHeaderLine('Content-Type'));
        $this->assertSame(self::TRACE, json_decode((string) $sent->getBody(), true)['items'][0]['traceId']);
    }

    public function testAnItemTheBinaryBodyDoesNotCarryLeavesThePushToJson(): void
    {
        // The binary body is not built; the JSON request then answers as it always did.
        $this->assertNull(PushBinary::body([['queue' => 'q', 'partition' => 'p', 'payload' => NAN]], false));
        $this->assertNull(PushBinary::body([['queue' => 'q', 'partition' => str_repeat('p', 70000), 'payload' => 1]], false));
        $this->assertNull(PushBinary::body([['queue' => 'q', 'partition' => 'p', 'payload' => 1, 'transactionId' => 5]], false));
        $this->assertNull(PushBinary::body([['queue' => 'q', 'partition' => 'p']], false));
        $this->assertNull(PushBinary::body([], false));
    }

    public function testABufferedPushIsBinaryToo(): void
    {
        $queen = $this->queen(self::queued(), ['binaryPush' => true]);

        $queen->queue('orders')->partition('p1')->buffer(['messageCount' => 10, 'timeMillis' => 60000])
            ->push(['data' => ['a' => 1]])->execute();
        $queen->flushAllBuffers();

        $this->assertCount(1, $this->pushes());
        $bytes = (string) $this->pushes()[0]->getBody();
        $this->assertSame('application/x-queen-push', $this->pushes()[0]->getHeaderLine('Content-Type'));
        $this->assertSame("QPB1\x04", substr($bytes, 0, 5));
        $this->assertSame('{"a":1}', self::plainGroups(substr($bytes, 5), true)[0]['items'][0][1]);
    }

    public function testTheCountsFlagAndMismatchedTransactionIds(): void
    {
        $batches = [['queue' => 'q', 'partition' => 'p', 'payloads' => ['1']]];
        $this->assertSame("QPB1\x02", substr(PushBinary::encode($batches, counts: true), 0, 5));
        $this->assertSame("QPB1\x00", substr(PushBinary::encode($batches), 0, 5));

        $this->expectExceptionMessage('1 transaction ids for 2 payloads');
        PushBinary::encode([['queue' => 'q', 'partition' => 'p', 'payloads' => ['1', '2'], 'transactionIds' => ['t']]]);
    }

    #[RequiresPhpExtension('zstd')]
    public function testTheZstdFlagCompressesTheWholeBodyWithItsRawLengthInFront(): void
    {
        $batches = [['queue' => 'q', 'partition' => 'p', 'payloads' => ['1']]];
        $plain = PushBinary::encode($batches);
        $packed = PushBinary::encode($batches, compress: true);

        $this->assertSame("QPB1\x01", substr($packed, 0, 5));
        $this->assertSame(strlen($plain) - 5, unpack('V', substr($packed, 5, 4))[1]);
        $this->assertSame(substr($plain, 5), Zstd::uncompress(substr($packed, 9)));
        $this->assertSame(8 | 2, ord(PushBinary::encodeFrames($batches, counts: true)[4]));
    }

    #[RequiresPhpExtension('zstd')]
    public function testPushCompressionSendsEachPartitionAsOneZstdFrameOfStoredFrames(): void
    {
        $queen = $this->queen(self::queued(3), ['binaryPush' => true, 'pushCompression' => true]);

        $queen->queue('orders')->partition('p1')->push([
            ['data' => ['a' => 1], 'transactionId' => self::TXN],
            ['data' => ['a' => 2]],
            ['data' => 'x'],
        ])->execute();

        $sent = $this->pushes()[0];
        $this->assertSame('application/x-queen-push', $sent->getHeaderLine('Content-Type'));
        $bytes = (string) $sent->getBody();
        $this->assertSame("QPB1\x08", substr($bytes, 0, 5));
        [$group] = self::framedGroups(substr($bytes, 5));
        $this->assertSame(['orders', 'p1', 3], [$group['queue'], $group['partition'], $group['count']]);
        $this->assertSame(['{"a":1}', '{"a":2}', '"x"'], array_column($group['frames'], 'payload'));
        $this->assertSame(self::TXN, $group['frames'][0]['txn']);
        foreach ($group['frames'] as $frame) {
            $this->assertSame(0, $frame['flags']);
            $this->assertSame(7, ord($frame['id'][6]) >> 4, 'the message id is a version-7 UUID');
            $this->assertSame(0x80, ord($frame['id'][8]) & 0xc0);
            $this->assertSame(36, strlen($frame['txn']));
        }
        $this->assertCount(3, array_unique(array_column($group['frames'], 'id')));
    }

    #[RequiresPhpExtension('zstd')]
    public function testAFrameWithoutATransactionIdGetsTheTextOfItsIdAndCarriesItsContentSize(): void
    {
        $bytes = PushBinary::encodeFrames([['queue' => 'q', 'partition' => 'p', 'payloads' => ['{"a":1}']]]);
        $frame = self::framedGroups(substr($bytes, 5))[0]['frames'][0];
        $this->assertSame(PushBinary::uuidText($frame['id']), $frame['txn']);

        // The broker asks for the content size of a frame it stores as it came.
        $at = 5 + 4 + 3 + 3 + 4;
        $zstd = self::take($bytes, $at, self::u32($bytes, $at));
        $this->assertSame(0xfd2fb528, unpack('V', substr($zstd, 0, 4))[1], 'a zstd frame');
        // Frame header descriptor: the single-segment bit, or a content-size
        // field of 2 bytes or more, says the frame carries its content size.
        $descriptor = ord($zstd[4]);
        $this->assertTrue(($descriptor & 0x20) !== 0 || ($descriptor >> 6) !== 0);
    }

    public function testTheZstdOptionsSayWhatTheyNeedWithoutTheExtension(): void
    {
        if (Zstd::available()) {
            $this->assertInstanceOf(Queen::class, $this->queen(self::queued(), ['binaryPush' => true, 'pushCompression' => true, 'framedPop' => true]));

            return;
        }
        // pushCompression alone compresses nothing: it is binaryPush that sends frames.
        $this->assertInstanceOf(Queen::class, $this->queen(self::queued(), ['pushCompression' => true]));
        foreach ([['binaryPush' => true, 'pushCompression' => true], ['framedPop' => true]] as $config) {
            try {
                $this->queen(self::queued(), $config);
                $this->fail('a client that needs zstd was built without it');
            } catch (\RuntimeException $error) {
                $this->assertStringContainsString('zstd extension', $error->getMessage());
            }
        }
    }

    public function testTheWireOptionsAreBooleans(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('binaryPush must be a boolean');
        $this->queen(self::queued(), ['binaryPush' => 'yes']);
    }

    // ===========================
    // Building a framed pop answer
    // ===========================

    private static function text(string $text): string
    {
        return pack('v', strlen($text)) . $text;
    }

    /** One stored frame: u32 body_len | flags | id | [trace] | txn | [producer] | [headers] | payload. */
    private static function storedFrame(int $id, string $txn, string $payload, ?string $producer = null, ?string $trace = null, ?string $headers = null): string
    {
        $idBytes = chr($id) . str_repeat("\0", 5) . "\x70\0\x80" . str_repeat("\0", 7);
        $body = chr(($trace !== null ? 1 : 0) | ($producer !== null ? 2 : 0) | ($headers !== null ? 8 : 0)) . $idBytes
            . ($trace !== null ? hex2bin(str_replace('-', '', $trace)) : '')
            . self::text($txn)
            . ($producer !== null ? self::text($producer) : '')
            . ($headers !== null ? self::text($headers) : '')
            . $payload;

        return pack('V', strlen($body)) . $body;
    }

    /**
     * A framed pop answer of one claim on partition 7 ("p7") of "orders",
     * lease "lease-1": one record at base offset 10 holding $frames, created
     * at 2023-11-14T22:13:20Z, of which the claim covers start..=end.
     *
     * @param list<string> $frames
     */
    private static function framedAnswer(string $own, int $start, int $end, bool $compress, array $frames): string
    {
        $raw = implode('', $frames);
        if ($compress) {
            $raw = Zstd::compress($raw);
        }

        return "QPF1\x01" . pack('V', strlen($own)) . $own
            . self::text('orders') . self::text('lease-1') . self::text('__QUEUE_MODE__')
            . pack('V', 1)                      // claims
            . pack('P', 7) . self::text('p7') . pack('P', $start) . pack('P', $end)
            . pack('V', 3)                      // delivery attempt
            . pack('V', 1)                      // records
            . pack('P', 10) . pack('P', 1_700_000_000_000_000) . pack('V', count($frames))
            . chr($compress ? 1 : 0) . pack('V', strlen($raw)) . $raw;
    }

    /** @return list<string> */
    private static function twoFrames(): array
    {
        return [
            self::storedFrame(1, 't-10', '{"n":10}'),
            self::storedFrame(2, 't-11', '{"n":11,"s":"x"}', producer: 'svc'),
        ];
    }

    /** @return array<string, array{bool}> */
    public static function codecs(): array
    {
        return ['a raw record' => [false], 'a zstd record' => [true]];
    }

    private function needZstd(bool $compress = true): void
    {
        if ($compress && !Zstd::available()) {
            $this->markTestSkipped('needs the zstd extension');
        }
    }

    // ===========================
    // The decoder
    // ===========================

    #[DataProvider('codecs')]
    public function testDecodeUnpacksARecordIntoTheJsonAnswersArray(bool $compress): void
    {
        $this->needZstd($compress);
        $got = PopFrames::decode(self::framedAnswer('{"frames":true}', 10, 11, $compress, self::twoFrames()));

        // assertSame on arrays compares keys, values, types and order.
        $this->assertSame(json_decode(self::TWO_MESSAGES_JSON, true), $got);
    }

    #[DataProvider('codecs')]
    public function testDecodeKeepsOnlyTheMessagesInsideTheClaim(bool $compress): void
    {
        $this->needZstd($compress);
        $got = PopFrames::decode(self::framedAnswer('{"frames":true}', 11, 11, $compress, self::twoFrames()));

        $this->assertCount(1, $got['messages']);
        $message = $got['messages'][0];
        $this->assertSame([11, 't-11', 'svc'], [$message['offset'], $message['transactionId'], $message['producerSub']]);
        $this->assertSame('02000000-0000-7000-8000-000000000000', $message['id']);
    }

    public function testDecodeReadsATraceIdAnEmptyPayloadAndAnAnswerWithoutOwnKeys(): void
    {
        $answer = self::framedAnswer('', 10, 11, false, [
            self::storedFrame(1, 't-10', '', trace: self::TRACE),
            self::storedFrame(2, 't-11', '[1,"due"]'),
        ]);
        $got = PopFrames::decode($answer);

        $this->assertSame(self::TRACE, $got['messages'][0]['traceId']);
        $this->assertNull($got['messages'][0]['data']);
        $this->assertSame([1, 'due'], $got['messages'][1]['data']);

        // Version 0 has no JSON object in front of the names.
        $this->assertSame($got, PopFrames::decode("QPF1\x00" . substr($answer, 9)));
    }

    public function testDecodeAddsTheAnswersOwnKeysWithoutFrames(): void
    {
        $own = '{"frames":true,"conflation":true,"autopilot":{"partitions":4,"batch":50}}';
        $got = PopFrames::decode(self::framedAnswer($own, 10, 11, false, self::twoFrames()));

        $this->assertArrayNotHasKey('frames', $got);
        $this->assertTrue($got['conflation']);
        $this->assertSame(['partitions' => 4, 'batch' => 50], $got['autopilot']);
    }

    public function testDecodeReadsAnUnsigned64BitPartitionId(): void
    {
        $answer = self::framedAnswer('', 10, 11, false, self::twoFrames());
        // The partition id sits after the three names and the claim count.
        $at = 9 + 8 + 9 + 16 + 4;
        $answer = substr_replace($answer, str_repeat("\xff", 8), $at, 8);

        $this->assertSame('18446744073709551615', PopFrames::decode($answer)['messages'][0]['partitionId']);
    }

    public function testDecodeRejectsDamage(): void
    {
        $frames = self::twoFrames();
        $answer = self::framedAnswer('{"frames":true}', 10, 11, false, $frames);
        $tail = strlen($frames[0]) + strlen($frames[1]);

        $this->assertTrue(PopFrames::isFrames($answer));
        $this->assertFalse(PopFrames::isFrames(self::TWO_MESSAGES_JSON));

        $future = $answer;
        $future[4] = chr(9);
        // A record that declares more frames than it holds.
        $short = substr_replace($answer, pack('V', 3), strlen($answer) - $tail - 9, 4);
        $badCodec = $answer;
        $badCodec[strlen($answer) - $tail - 5] = chr(7);

        foreach ([
            'truncated' => substr($answer, 0, -3),
            'not a framed pop answer' => self::TWO_MESSAGES_JSON,
            'unknown version 9' => $future,
            'a record is shorter than its 3 frames' => $short,
            'unknown codec 7' => $badCodec,
        ] as $message => $damaged) {
            try {
                PopFrames::decode($damaged);
                $this->fail("decoded an answer that is damaged: {$message}");
            } catch (\UnexpectedValueException $error) {
                $this->assertStringContainsString($message, $error->getMessage());
            }
        }
    }

    // ===========================
    // Pop
    // ===========================

    public function testAPopSendsNoFramesParameterByDefault(): void
    {
        $queen = $this->queen(self::json());

        $messages = $queen->queue('orders')->batch(2)->wait(false)->pop();

        $this->assertCount(2, $messages);
        $this->assertNull(self::param($this->pops()[0], 'frames'));
    }

    #[RequiresPhpExtension('zstd')]
    #[DataProvider('codecs')]
    public function testAFramedPopGivesTheMessagesOfTheJsonAnswer(bool $compress): void
    {
        $want = $this->queen(self::json())->queue('orders')->batch(2)->wait(false)->pop();

        $answer = self::framedAnswer('{"frames":true}', 10, 11, $compress, self::twoFrames());
        $got = $this->queen(self::framed($answer), ['framedPop' => true])->queue('orders')->batch(2)->wait(false)->pop();

        $this->assertSame('true', self::param($this->pops()[0], 'frames'));
        $this->assertCount(2, $got);
        $this->assertSame($want, $got);
    }

    #[RequiresPhpExtension('zstd')]
    public function testAFramedPopAsksForFramesOnEveryRouteAndEveryWayOfPopping(): void
    {
        $queen = $this->queen(self::nothing(), ['framedPop' => true]);

        $queen->queue('orders')->partition('p7')->wait(false)->pop();
        $queen->queue()->namespace('ns')->wait(false)->pop();
        // Sent ahead and settled later, as the Laravel worker pops.
        $builder = $queen->queue('orders')->wait(false);
        $this->assertSame([], $builder->settlePop($builder->popDetached()));
        // The poll of the consumer handle.
        $consumer = $queen->queue('orders')->group('workers')->getConsumer();
        $consumer->subscribe();
        $consumer->consume(10);

        $pops = $this->pops();
        $this->assertCount(4, $pops);
        $this->assertSame('/api/v1/pop/queue/orders/partition/p7', $pops[0]->getUri()->getPath());
        $this->assertSame('/api/v1/pop', $pops[1]->getUri()->getPath());
        foreach ($pops as $pop) {
            $this->assertSame('true', self::param($pop, 'frames'), $pop->getUri()->getPath());
        }
    }

    #[RequiresPhpExtension('zstd')]
    public function testAFramedPopSentAheadIsUnpackedWhenItIsSettled(): void
    {
        $answer = self::framedAnswer('{"frames":true}', 10, 11, true, self::twoFrames());
        $builder = $this->queen(self::framed($answer), ['framedPop' => true])->queue('orders')->batch(2)->wait(false);

        $messages = $builder->settlePop($builder->popDetached());

        $this->assertSame(['t-10', 't-11'], array_column($messages, 'transactionId'));
    }

    #[RequiresPhpExtension('zstd')]
    public function testAFramedPopReadsAJsonAnswerAsAlways(): void
    {
        // A broker that does not know the parameter answers JSON.
        $got = $this->queen(self::json(), ['framedPop' => true])->queue('orders')->batch(2)->wait(false)->pop();

        $this->assertCount(2, $got);
        $this->assertSame('t-11', $got[1]['transactionId']);
        $this->assertSame('x', $got[1]['data']['s']);
    }

    #[RequiresPhpExtension('zstd')]
    public function testAFramedPopOfNothing(): void
    {
        $this->assertSame([], $this->queen(self::nothing(), ['framedPop' => true])->queue('orders')->wait(false)->pop());
    }

    #[RequiresPhpExtension('zstd')]
    public function testAFramedPopCarriesTheAutopilotsChoice(): void
    {
        $own = '{"frames":true,"autopilot":{"partitions":4,"batch":50}}';
        $answer = self::framedAnswer($own, 10, 11, false, self::twoFrames());

        $result = $this->queen(self::framed($answer), ['framedPop' => true])->queue('orders')->wait(false)->popResult();

        $this->assertCount(2, $result['messages']);
        $this->assertSame(4, $result['autopilot']['partitions']);
        $this->assertSame(50, $result['autopilot']['batch']);
    }

    #[RequiresPhpExtension('zstd')]
    public function testAFramedPopCarriesTheConflationEchoAndStillThrowsWithoutIt(): void
    {
        $echoed = self::framedAnswer('{"frames":true,"conflation":true}', 10, 11, false, self::twoFrames());
        $got = $this->queen(self::framed($echoed), ['framedPop' => true])
            ->queue('orders')->group('g')->conflation(true)->wait(false)->pop();
        $this->assertCount(2, $got);

        $silent = self::framedAnswer('{"frames":true}', 10, 11, false, self::twoFrames());
        $this->expectException(ConflationUnsupportedException::class);
        $this->queen(self::framed($silent), ['framedPop' => true])
            ->queue('orders')->group('g2')->conflation(true)->wait(false)->pop();
    }

    #[RequiresPhpExtension('zstd')]
    public function testConsumeWithFramedPopsHandsTheHandlerTheClaimInOrder(): void
    {
        $answer = self::framedAnswer('{"frames":true}', 10, 11, true, self::twoFrames());
        $queen = $this->queen(self::framed($answer), ['framedPop' => true]);
        $seen = [];

        $queen->queue('orders')->batch(2)->wait(false)->autoAck(false)->each()->limit(2)
            ->consume(function (array $message) use (&$seen): void {
                $seen[] = "{$message['transactionId']}@{$message['partitionId']}/{$message['leaseId']}";
            })
            ->execute();

        $this->assertSame(['t-10@7/lease-1', 't-11@7/lease-1'], $seen);
        $this->assertNotEmpty($this->pops());
        foreach ($this->pops() as $pop) {
            $this->assertSame('true', self::param($pop, 'frames'));
        }
    }

    // ===========================
    // Transaction
    // ===========================
    //
    // With binaryPush and pushCompression a bundle that pushes goes out as the
    // binary transaction body. What is at stake is the default (a client that
    // does not set both sends the JSON bundle every released broker knows)
    // and, with the options, the order of the results: the broker lists the
    // head's acks before the pushes, and the caller must get them back in the
    // order of its own operations.

    private const POPPED = ['transactionId' => 'in-1', 'partitionId' => '7', 'leaseId' => 'lease-1'];
    private const FRAMED = ['binaryPush' => true, 'pushCompression' => true];

    private static function committed(array $results): \Closure
    {
        $body = json_encode(['transactionId' => 'tx', 'success' => true, 'results' => $results]);

        return static fn (): array => [200, 'application/json', $body];
    }

    private function onlyTransaction(): RequestInterface
    {
        $sent = array_values(array_filter(
            $this->requests,
            static fn (RequestInterface $r): bool => $r->getMethod() === 'POST' && $r->getUri()->getPath() === '/api/v1/transaction',
        ));
        $this->assertCount(1, $sent);

        return $sent[0];
    }

    /**
     * The flags, the head and the groups of a binary transaction body; a
     * group is [queue, partition, transaction ids, payloads].
     *
     * @return array{0: int, 1: ?array, 2: list<array{0: string, 1: string, 2: list<string>, 3: list<string>}>}
     */
    private static function transactionBody(string $raw): array
    {
        self::assertSame('QTB1', substr($raw, 0, 4));
        $headLength = unpack('V', substr($raw, 5, 4))[1];
        $head = substr($raw, 9, $headLength);
        $groups = array_map(
            static fn (array $g): array => [$g['queue'], $g['partition'], array_column($g['frames'], 'txn'), array_column($g['frames'], 'payload')],
            self::framedGroups(substr($raw, 9 + $headLength)),
        );

        return [ord($raw[4]), $head === '' ? null : json_decode($head, true, 512, JSON_THROW_ON_ERROR), $groups];
    }

    /** @return array<string, array{array}> */
    public static function notBothOptions(): array
    {
        return [
            'no option set' => [[]],
            'binaryPush alone' => [['binaryPush' => true]],
            'pushCompression alone' => [['pushCompression' => true]],
        ];
    }

    #[DataProvider('notBothOptions')]
    public function testATransactionIsTheJsonBundleByDefault(array $config): void
    {
        $queen = $this->queen(self::committed([
            ['index' => 0, 'type' => 'ack', 'success' => true],
            ['index' => 1, 'type' => 'push', 'success' => true],
        ]), $config);

        $queen->transaction()->ack(self::POPPED)->queue('out')->partition('p1')->push([['data' => ['n' => 1]]])->commit();

        $sent = $this->onlyTransaction();
        $this->assertSame('application/json', $sent->getHeaderLine('Content-Type'));
        $this->assertSame(['ack', 'push'], array_column(json_decode((string) $sent->getBody(), true)['operations'], 'type'));
    }

    #[RequiresPhpExtension('zstd')]
    public function testThePushesTravelAsFramesAndTheRestAsTheJsonHead(): void
    {
        $queen = $this->queen(self::committed([
            ['index' => 0, 'type' => 'ack', 'success' => true, 'transactionId' => 'in-1'],
            ['index' => 1, 'type' => 'push', 'success' => true, 'transactionId' => 'a'],
            ['index' => 2, 'type' => 'push', 'success' => true, 'transactionId' => 'tx-b'],
            ['index' => 3, 'type' => 'push', 'success' => true, 'transactionId' => 'c'],
        ]), self::FRAMED);

        $result = $queen->transaction()
            ->ack(self::POPPED)
            ->queue('out')->partition('p1')->push([['data' => ['n' => 1]]])
            ->queue('out')->partition('p1')->push([['data' => ['n' => 2], 'transactionId' => 'tx-b']])
            ->queue('out')->partition('p2')->push([['data' => 'text']])
            ->commit();

        $this->assertTrue($result['success']);
        $this->assertCount(4, $result['results']);
        $sent = $this->onlyTransaction();
        $this->assertSame('application/x-queen-push', $sent->getHeaderLine('Content-Type'));
        [$flags, $head, $groups] = self::transactionBody((string) $sent->getBody());
        $this->assertSame(0, $flags);
        // The head: the ack and the lease, and no push.
        $this->assertSame([
            'operations' => [
                ['type' => 'ack', 'transactionId' => 'in-1', 'partitionId' => '7', 'status' => 'completed', 'leaseId' => 'lease-1'],
            ],
            'requiredLeases' => ['lease-1'],
        ], $head);
        // The pushes: consecutive items of one partition share a group.
        $this->assertCount(2, $groups);
        $this->assertSame(['out', 'p1', ['{"n":1}', '{"n":2}']], [$groups[0][0], $groups[0][1], $groups[0][3]]);
        $this->assertSame(36, strlen($groups[0][2][0]));
        $this->assertSame('tx-b', $groups[0][2][1]);
        $this->assertSame(['out', 'p2', ['"text"']], [$groups[1][0], $groups[1][1], $groups[1][3]]);
    }

    #[RequiresPhpExtension('zstd')]
    public function testTheResultsComeBackInTheOrderOfTheCallersOperations(): void
    {
        // The caller pushes, acks, pushes. The broker answers the ack first (it
        // is in the head), then the two pushes, then the rider.
        $queen = $this->queen(self::committed([
            ['index' => 0, 'type' => 'ack', 'success' => true, 'transactionId' => 'in-1'],
            ['index' => 1, 'type' => 'push', 'success' => true, 'transactionId' => 'first'],
            ['index' => 2, 'type' => 'push', 'success' => true, 'transactionId' => 'second'],
            ['index' => 3, 'type' => 'kv', 'opIndex' => 0, 'applied' => true],
        ]), self::FRAMED);

        $result = $queen->transaction()
            ->queue('out')->push([['data' => 1, 'transactionId' => 'first']])
            ->ack(self::POPPED)
            ->queue('out')->push([['data' => 2, 'transactionId' => 'second']])
            ->kv('saga')->put('k1', ['step' => 1], ['ttlSeconds' => 86400])
            ->commit();

        $this->assertSame(
            [[0, 'push', 'first'], [1, 'ack', 'in-1'], [2, 'push', 'second'], [3, 'kv', null]],
            array_map(static fn (array $r): array => [$r['index'], $r['type'], $r['transactionId'] ?? null], $result['results']),
        );
        // On the wire the two pushes are one group, in the caller's order, and
        // the rider travels in the head.
        [, $head, $groups] = self::transactionBody((string) $this->onlyTransaction()->getBody());
        $this->assertSame(['ack'], array_column($head['operations'], 'type'));
        $this->assertCount(1, $head['kv']);
        $this->assertCount(1, $groups);
        $this->assertSame(['first', 'second'], $groups[0][2]);
        $this->assertSame('Default', $groups[0][1]);
    }

    #[RequiresPhpExtension('zstd')]
    public function testTheResultsStayAsTheyAreWhenTheAcksComeFirstAnyway(): void
    {
        $results = [
            ['index' => 0, 'type' => 'ack', 'success' => true, 'transactionId' => 'in-1'],
            ['index' => 1, 'type' => 'push', 'success' => true, 'transactionId' => 'a'],
        ];
        $queen = $this->queen(self::committed($results), self::FRAMED);

        $result = $queen->transaction()->ack(self::POPPED)
            ->queue('out')->push([['data' => 1, 'transactionId' => 'a']])->commit();

        $this->assertSame($results, $result['results']);
    }

    #[RequiresPhpExtension('zstd')]
    public function testARolledBackBundleComesBackAsTheBrokerAnsweredIt(): void
    {
        $lost = [
            'transactionId' => 'tx', 'success' => false, 'reason' => 'kv_precondition',
            'results' => [], 'failedIndex' => 2, 'kvReason' => 'exists',
        ];
        $queen = $this->queen(static fn (): array => [200, 'application/json', json_encode($lost)], self::FRAMED);

        $result = $queen->transaction()
            ->queue('out')->push([['data' => 1]])
            ->ack(self::POPPED)
            ->kv('idem')->putIfAbsent('order-1', true, ['ttlSeconds' => 86400, 'required' => true])
            ->commit();

        $this->assertSame($lost, $result);
        $this->assertSame('QTB1', substr((string) $this->onlyTransaction()->getBody(), 0, 4));
    }

    #[RequiresPhpExtension('zstd')]
    public function testABundleWhosePushCarriesATraceIdStaysJson(): void
    {
        $queen = $this->queen(self::committed([['index' => 0, 'type' => 'push', 'success' => true]]), self::FRAMED);

        $queen->transaction()->queue('out')->push([['data' => 1, 'traceId' => self::TRACE]])->commit();

        $sent = $this->onlyTransaction();
        $this->assertSame('application/json', $sent->getHeaderLine('Content-Type'));
        $this->assertSame(self::TRACE, json_decode((string) $sent->getBody(), true)['operations'][0]['items'][0]['traceId']);
    }

    #[RequiresPhpExtension('zstd')]
    public function testABundleWithoutPushesStaysJson(): void
    {
        // There is nothing to frame.
        $queen = $this->queen(self::committed([['index' => 0, 'type' => 'ack', 'success' => true]]), self::FRAMED);

        $queen->transaction()->ack(self::POPPED)->commit();

        $sent = $this->onlyTransaction();
        $this->assertSame('application/json', $sent->getHeaderLine('Content-Type'));
        $this->assertSame('{', ((string) $sent->getBody())[0]);
    }

    #[RequiresPhpExtension('zstd')]
    public function testTheTransactionCountsFlagAndAnEmptyHead(): void
    {
        $body = PushBinary::encodeTransactionFrames(null, [['queue' => 'q', 'partition' => 'p', 'payloads' => ['1']]], counts: true);

        $this->assertSame("QTB1\x02\x00\x00\x00\x00", substr($body, 0, 9));
        [$flags, $head, $groups] = self::transactionBody($body);
        $this->assertSame([2, null], [$flags, $head]);
        $this->assertSame([['1']], array_column($groups, 3));
    }

    public function testTransactionFramesWithoutAPushOrWithOneItCannotCarryIsNull(): void
    {
        $ack = ['type' => 'ack', 'transactionId' => 'in-1', 'partitionId' => '7', 'status' => 'completed'];
        $this->assertNull(PushBinary::transactionFrames(['operations' => [$ack], 'requiredLeases' => []]));
        $this->assertNull(PushBinary::transactionFrames(['operations' => []]));
        $this->assertNull(PushBinary::transactionFrames(['kv' => []]));
        $this->assertNull(PushBinary::transactionFrames(['operations' => [
            ['type' => 'push', 'items' => [['queue' => 'q', 'payload' => 1, 'traceId' => self::TRACE]]],
        ]]));
        // The order the broker answers in, when it is not the caller's.
        $this->assertSame(
            ['index' => 1, 'type' => 'ack'],
            PushBinary::restoreResultOrder(
                ['results' => [['index' => 0, 'type' => 'ack'], ['index' => 1, 'type' => 'push']]],
                [1, 0],
            )['results'][1],
        );
    }

    // ===========================
    // Headers
    // ===========================
    //
    // A stored frame carries a message's headers behind flag 8, as the
    // broker checks a client's frame (rsm/facade/real.rs, client_frames): the
    // block right after the transaction id, its names sorted, and nothing a
    // second encoding would write differently. The plain binary body has no
    // place for them, so a push with headers goes as JSON there.

    public function testTheHeadersBlockIsSortedByNameAsTheBrokerWritesIt(): void
    {
        $block = PushBinary::headersBlock(['type' => 'order.paid', 'a' => 'x', '10' => 'y', '9' => 'z', 'traceparent' => '']);

        // In byte order: "10" < "9" < "a" < "traceparent" < "type".
        $this->assertSame(
            pack('v', 5)
            . "\x02" . '10' . pack('v', 1) . 'y'
            . "\x01" . '9' . pack('v', 1) . 'z'
            . "\x01" . 'a' . pack('v', 1) . 'x'
            . "\x0b" . 'traceparent' . pack('v', 0)
            . "\x04" . 'type' . pack('v', 10) . 'order.paid',
            $block,
        );
        $this->assertSame(['10' => 'y', '9' => 'z', 'a' => 'x', 'traceparent' => '', 'type' => 'order.paid'], self::headersOf($block));
    }

    public function testHeadersTheBlockCannotCarryLeaveItToJson(): void
    {
        $this->assertNull(PushBinary::headersBlock(['type' => "\xff\xfe"]), 'not UTF-8');
        $this->assertNull(PushBinary::headersBlock([str_repeat('n', 256) => 'v']), 'a name over 255 bytes');
        $this->assertNull(PushBinary::headersBlock(['big' => str_repeat('v', 65536)]), 'a value over 65535 bytes');
        $this->assertNull(PushBinary::headersBlock(['a' => str_repeat('v', 40000), 'b' => str_repeat('v', 40000)]), 'a block over 65535 bytes');
        $this->assertNull(PushBinary::headersBlock(['n' => 7]), 'a value that is not text');
    }

    public function testAFrameWithHeadersCarriesFlag8AndItsBlockAfterTheTransactionId(): void
    {
        $id = str_repeat("\x11", 6) . "\x70\x00\x80" . str_repeat("\x00", 7);
        $block = PushBinary::headersBlock(['type' => 'order.paid']);

        $bytes = PushBinary::frame($id, 'tx-1', '{"a":1}', $block);
        $at = 0;
        $this->assertSame(
            ['flags' => 8, 'id' => $id, 'txn' => 'tx-1', 'headers' => $block, 'payload' => '{"a":1}'],
            self::unframe($bytes, $at),
        );
        $this->assertSame(strlen($bytes), $at, 'body_len counts the block');
        $this->assertSame(['type' => 'order.paid'], self::headersOf($block));

        // Without headers, the frame this client always built.
        $this->assertSame(
            pack('V', 17 + 2 + 4 + 7) . "\0" . $id . pack('v', 4) . 'tx-1' . '{"a":1}',
            PushBinary::frame($id, 'tx-1', '{"a":1}'),
        );
    }

    public function testAPlainBinaryPushWithHeadersStaysJson(): void
    {
        $queen = $this->queen(self::queued(), ['binaryPush' => true]);

        $queen->queue('orders')->partition('p1')->push(['data' => ['a' => 1], 'headers' => ['type' => 'x']])->execute();

        $sent = $this->pushes()[0];
        $this->assertSame('application/json', $sent->getHeaderLine('Content-Type'));
        $this->assertSame(['type' => 'x'], json_decode((string) $sent->getBody(), true)['items'][0]['headers']);
        $item = ['queue' => 'q', 'partition' => 'p', 'payload' => 1];
        $this->assertNull(PushBinary::body([$item + ['headers' => ['type' => 'x']]], false));
        $this->assertNotNull(PushBinary::body([$item + ['headers' => []]], false), 'no headers is none');

        $this->expectException(\InvalidArgumentException::class);
        PushBinary::encode([['queue' => 'q', 'partition' => 'p', 'payloads' => ['1'], 'headerBlocks' => [PushBinary::headersBlock(['type' => 'x'])]]]);
    }

    public function testPushCompressionFramesTheHeadersOfEachMessage(): void
    {
        $this->needZstd();
        $queen = $this->queen(self::queued(2), self::FRAMED);

        $queen->queue('orders')->partition('p1')->push([
            ['data' => ['a' => 1], 'transactionId' => self::TXN, 'headers' => ['type' => 'order.paid', 'customer' => 42]],
            ['data' => ['a' => 2]],
        ])->execute();

        $sent = $this->pushes()[0];
        $this->assertSame('application/x-queen-push', $sent->getHeaderLine('Content-Type'));
        $this->assertSame("QPB1\x08", substr((string) $sent->getBody(), 0, 5));
        [$group] = self::framedGroups(substr((string) $sent->getBody(), 5));
        [$first, $second] = $group['frames'];
        $this->assertSame([8, self::TXN, '{"a":1}'], [$first['flags'], $first['txn'], $first['payload']]);
        $this->assertSame(['customer' => '42', 'type' => 'order.paid'], self::headersOf($first['headers']));
        $this->assertSame([0, null, '{"a":2}'], [$second['flags'], $second['headers'], $second['payload']]);
    }

    public function testABundleFramesItsPushesHeadersAndKeepsItsLinksInTheHead(): void
    {
        $this->needZstd();
        // The caller pushes, then links. The broker answers the head's link
        // first, then the push.
        $queen = $this->queen(self::committed([
            ['index' => 0, 'type' => 'link', 'success' => true, 'transactionId' => 'in-1'],
            ['index' => 1, 'type' => 'push', 'success' => true, 'transactionId' => 'out-1'],
        ]), self::FRAMED);
        $popped = self::POPPED + ['offset' => 10, 'consumerGroup' => 'workers'];

        $result = $queen->transaction()
            ->queue('out')->partition('p1')->push([['data' => ['n' => 1], 'transactionId' => 'out-1', 'headers' => ['type' => 'x']]])
            ->queue('review')->partition('p7')->link($popped, ['review-reason' => 'amount'])
            ->commit();

        $this->assertSame(
            [[0, 'push', 'out-1'], [1, 'link', 'in-1']],
            array_map(static fn (array $r): array => [$r['index'], $r['type'], $r['transactionId']], $result['results']),
        );
        $raw = (string) $this->onlyTransaction()->getBody();
        [, $head] = self::transactionBody($raw);
        $this->assertSame([[
            'type' => 'link',
            'queue' => 'review',
            'partition' => 'p7',
            'from' => ['partitionId' => '7', 'offset' => 10, 'transactionId' => 'in-1', 'consumerGroup' => 'workers'],
            'headers' => ['review-reason' => 'amount'],
        ]], $head['operations']);
        [$group] = self::framedGroups(substr($raw, 9 + unpack('V', substr($raw, 5, 4))[1]));
        $this->assertSame(['out', 'p1'], [$group['queue'], $group['partition']]);
        $this->assertSame(8, $group['frames'][0]['flags']);
        $this->assertSame(['type' => 'x'], self::headersOf($group['frames'][0]['headers']));
    }

    public function testDecodeReadsHeadersWhereTheJsonAnswerHasThem(): void
    {
        $block = PushBinary::headersBlock(['type' => 'order.paid', 'customer' => 'c-7']);
        $answer = self::framedAnswer('{"frames":true}', 10, 11, false, [
            self::storedFrame(1, 't-10', '{"n":10}'),
            self::storedFrame(2, 't-11', '{"n":11,"s":"x"}', producer: 'svc', headers: $block),
        ]);
        // The same two messages as the JSON answer has them: the second's
        // headers right after its producerSub.
        $json = str_replace(
            '"producerSub":"svc",',
            '"producerSub":"svc","headers":{"customer":"c-7","type":"order.paid"},',
            self::TWO_MESSAGES_JSON,
        );

        $this->assertSame(json_decode($json, true), PopFrames::decode($answer));
    }

    /** A framed answer says it filtered in its own keys, which decode() keeps. */
    public function testAFramedPopOfAFilteredGroupReadsTheEchoFromTheAnswersOwnKeys(): void
    {
        $this->needZstd();
        $echoed = self::framedAnswer('{"frames":true,"filtered":true}', 10, 11, false, self::twoFrames());
        $got = $this->queen(self::framed($echoed), ['framedPop' => true])
            ->queue('orders')->group('it')->where('data.n', '>', 1)->wait(false)->pop();
        $this->assertSame(['t-10', 't-11'], array_column($got, 'transactionId'));

        $silent = self::framedAnswer('{"frames":true}', 10, 11, false, self::twoFrames());
        $this->expectException(\Queen\Exceptions\RoutingUnsupportedException::class);
        $this->queen(self::framed($silent), ['framedPop' => true])
            ->queue('orders')->group('it')->where('data.n', '>', 1)->wait(false)->pop();
    }

    public function testDecodeRefusesALinkFrame(): void
    {
        $frame = self::storedFrame(1, 't-10', '{"n":10}');
        // The flags byte, right after body_len: a link the broker did not resolve.
        $frame[4] = chr(16);

        $this->expectException(\UnexpectedValueException::class);
        $this->expectExceptionMessage('a frame is a link');
        PopFrames::decode(self::framedAnswer('', 10, 10, false, [$frame]));
    }
}
