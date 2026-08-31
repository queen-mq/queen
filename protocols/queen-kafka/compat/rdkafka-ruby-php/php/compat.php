<?php
/**
 * php-rdkafka (pecl) against the queen-kafka facade.
 *
 *   php compat.php [bootstrap] [runId]
 *
 * WHAT THIS PROVES, AND WHAT IT DOES NOT
 *
 * The librdkafka core is already covered by compat/librdkafka; re-proving the wire
 * protocol here would be theatre. What is NOT covered elsewhere is the PHP
 * PACKAGING and this extension's own surface:
 *
 *   * php-rdkafka does NOT vendor librdkafka. It links whatever the system
 *     provides, so the C core's version -- and therefore the whole feature set --
 *     is a decision made by the Dockerfile/distro, not by the extension. Section 0
 *     prints the build-time AND runtime librdkafka so a mismatch is visible.
 *   * whether producev()'s headers, keys and binary payloads survive the facade
 *     byte-for-byte,
 *   * whether the extension's blocking-call surface (consume(), commit(),
 *     queryWatermarkOffsets(), getCommittedOffsets()) behaves against a facade
 *     that is not Apache Kafka.
 *
 * BEHAVIOURS THAT ARE THE CLIENT'S FAULT, NOT THE FACADE'S
 *
 *   * NO SEEK. php-rdkafka 6.0.5's KafkaConsumer has assign(), incrementalAssign()
 *     and getOffsetPositions(), but no seek() at all -- unlike the Ruby gem, which
 *     has both seek() and seek_by(). Section 6 therefore repositions with
 *     assign(TopicPartition(topic, p, offset)), which is the supported idiom and
 *     issues the same Fetch. This is an extension gap, not a facade gap.
 *   * zstd. librdkafka gates the zstd codec on Fetch v10; queen-kafka caps Fetch at
 *     v6 on purpose (v7 is fetch sessions, KIP-227). librdkafka therefore decides
 *     the broker "does not support" zstd and produces the batch UNCOMPRESSED rather
 *     than failing. Records still land and round-trip byte-exact.
 *   * enable.idempotence. Advertised and enforced since M7 F3, so nothing here
 *     needs it off any more. librdkafka defaults it off and this suite keeps it
 *     there explicitly, so the run does not depend on a default.
 *   * Delivery reports arrive only while poll() runs. A PHP script that produces
 *     and exits without flush() loses them -- that is the extension's threading
 *     model, and every produce here is flushed.
 *
 * Every blocking call has a deadline. A hang is a result.
 */

declare(strict_types=1);

$BOOTSTRAP = $argv[1] ?? '127.0.0.1:19092';
$RUN       = $argv[2] ?? (string) time();

const NMAIN     = 512;   // the bar: >= 500 across >= 4 partitions
const NCODEC    = 512;
const NZSTD     = 128;
const RESUME_AT = 200;

$PARTITIONS   = (int) (getenv('KAFKA_PARTITIONS') ?: '8');
$TOPIC_MAIN   = "phprdk-$RUN";
$TOPIC_LZ4    = "phprdk-lz4-$RUN";
$TOPIC_ZSTD   = "phprdk-zstd-$RUN";
$TOPIC_AUTO   = "phprdk-auto-$RUN";
$GROUP_MAIN   = "phprdk-g-$RUN";
$GROUP_CODEC  = "phprdk-gc-$RUN";
$GROUP_RESUME = "phprdk-gr-$RUN";
$TRACE        = getenv('NEGOTIATED_TRACE_FILE') ?: (sys_get_temp_dir() . "/phprdk-$RUN.trace.log");

// ------------------------------------------------------------------ reporting
$FAIL = 0;
function say(string $s): void  { printf("\n=== %s\n", $s); }
function ok(string $s): void   { printf("  ok   %s\n", $s); }
function info(string $s): void { printf("  ..   %s\n", $s); }
function bad(string $s): void  { global $FAIL; $FAIL++; printf("  FAIL %s\n", $s); }
function check(bool $c, string $s): bool { $c ? ok($s) : bad($s); return $c; }
function now(): float { return hrtime(true) / 1e9; }

// ------------------------------------------------------------------ fixture
// 256 raw bytes, every value 0x00..0xff, so "byte-exact" means what it says: NULs,
// high bytes, anything a UTF-8 assumption would mangle. Identical in every message,
// which also makes the payload highly compressible -- that is what lets section 5
// tell a real lz4 batch from a downgraded one.
$BLOB = '';
for ($b = 0; $b < 256; $b++) { $BLOB .= chr($b); }

function payload_for(int $i): string {
    global $BLOB;
    return sprintf('idx=%06d;', $i) . $BLOB . sprintf(';fin=%06d', $i);
}
function key_for(int $i): string { return sprintf('k-%06d', $i); }
function headers_for(int $i): array {
    global $RUN;
    return [
        'idx'   => sprintf('%06d', $i),
        'trace' => "phprdk-$RUN",
        'uni'   => "h\xc3\xa9llo-\xc3\xbcn\xc3\xafcode",  // multibyte UTF-8
        'empty' => '',                                     // empty != null on the wire
    ];
}
// Every message carries its own index, so a consumer rebuilds what was produced
// without trusting arrival order; per-partition order is then a property of the
// recovered index sequence rather than of the loop that produced it.
function index_of(?string $payload): ?int {
    if ($payload === null) { return null; }
    return preg_match('/idx=(\d{6});/', substr($payload, 0, 16), $m) ? (int) $m[1] : null;
}

// ------------------------------------------------------------------ config
$TRACE_FH = fopen($TRACE, 'w');

function base_conf(array $extra = []): RdKafka\Conf {
    global $BOOTSTRAP, $RUN, $TRACE_FH;
    $conf = new RdKafka\Conf();
    $conf->set('bootstrap.servers', $BOOTSTRAP);
    $conf->set('client.id', "phprdk-$RUN");
    // librdkafka defaults this off and this suite keeps it off on purpose; say
    // it anyway so the suite does not silently depend on that default holding.
    // Since M7 F3 the facade would accept it on.
    $conf->set('enable.idempotence', 'false');
    // Read the negotiated API versions out of librdkafka's own mouth rather than
    // assuming them (compat/ convention 5). php-rdkafka forwards librdkafka's log
    // queue into the main queue, so these arrive during poll()/consume().
    // `protocol` AND `msg`. protocol is what section 9 reads the negotiated
    // versions out of. msg is NOT optional decoration: librdkafka's
    // "Broker does not support compression type X" line is emitted through the
    // MSG debug facility, so with `debug=protocol` alone the compression
    // downgrade is INVISIBLE to this suite -- measured, both facilities side by
    // side against the same facade: protocol=no notice, msg=notice seen. That
    // blindness is what section 5 and section 9 assert against.
    $conf->set('debug', 'protocol,msg');
    $conf->set('log_level', (string) LOG_DEBUG);
    $conf->setLogCb(function ($kafka, int $level, string $facility, string $message) use ($TRACE_FH) {
        fwrite($TRACE_FH, "$level $facility $message\n");
    });
    foreach ($extra as $k => $v) { $conf->set($k, (string) $v); }
    return $conf;
}

function producer_conf(array $extra = []): RdKafka\Conf {
    return base_conf(array_merge(['acks' => 'all'], $extra));
}

// NOTE: the consumer conf deliberately leaves session.timeout.ms,
// heartbeat.interval.ms and partition.assignment.strategy at librdkafka's stock
// values. Testing the DEFAULTS is the point of this row of the matrix.
function consumer_conf(string $group, array $extra = []): RdKafka\Conf {
    return base_conf(array_merge(['group.id' => $group, 'auto.offset.reset' => 'earliest'], $extra));
}

function dump_conf(string $label, array $h): void {
    $parts = [];
    foreach ($h as $k => $v) { $parts[] = "$k=$v"; }
    sort($parts);
    info("$label: " . implode(' ', $parts));
}

// ------------------------------------------------------------------ helpers
/**
 * Produce a range of indices and wait for every delivery report. Returns
 * [reports_by_index, error_count]. acks=all, so the offsets in the reports are the
 * broker's numbers and not the client's own counter.
 */
function produce_range(string $topicName, array $confExtra, int $from, int $count, int $partitions): array {
    $reports = [];
    $errors  = 0;
    $conf = producer_conf($confExtra);
    $conf->setDrMsgCb(function ($kafka, RdKafka\Message $msg) use (&$reports, &$errors) {
        if ($msg->err) { $errors++; if ($errors <= 3) { bad("delivery report error: " . $msg->errstr()); } return; }
        $i = index_of($msg->payload);
        if ($i === null) { $errors++; return; }
        $reports[$i] = ['partition' => $msg->partition, 'offset' => $msg->offset];
    });
    $producer = new RdKafka\Producer($conf);
    $topic    = $producer->newTopic($topicName);
    for ($i = $from; $i < $from + $count; $i++) {
        $topic->producev(
            $i % $partitions,
            RD_KAFKA_MSG_F_BLOCK,
            payload_for($i),
            key_for($i),
            headers_for($i)
        );
        // The extension serves delivery reports and log events only inside poll();
        // draining as we go keeps the out-queue and the trace file from bunching.
        $producer->poll(0);
    }
    $deadline = now() + 30;
    while ($producer->getOutQLen() > 0 && now() < $deadline) { $producer->poll(100); }
    $producer->flush(10000);
    $producer->poll(0);
    if ($producer->getOutQLen() > 0) { bad("producer still had {$producer->getOutQLen()} messages queued after flush"); }
    return [$reports, $errors];
}

/**
 * Consume until $want messages arrive or the deadline passes. PARTITION_EOF and
 * TIMED_OUT are not failures -- they are how librdkafka says "nothing right now".
 * A short deadline is what turns a facade hang into a FAIL line instead of a suite
 * that never returns.
 */
function drain(RdKafka\KafkaConsumer $c, int $want, int $secs): array {
    $got  = [];
    $stop = now() + $secs;
    while (count($got) < $want && now() < $stop) {
        $m = $c->consume(1000);
        if ($m === null) { continue; }
        if ($m->err === RD_KAFKA_RESP_ERR_NO_ERROR) { $got[] = $m; continue; }
        if ($m->err === RD_KAFKA_RESP_ERR__PARTITION_EOF) { continue; }
        if ($m->err === RD_KAFKA_RESP_ERR__TIMED_OUT) { continue; }
        bad("consume returned error {$m->err}: " . $m->errstr());
        break;
    }
    return $got;
}

function headers_of(RdKafka\Message $m): array {
    $h = $m->headers ?? [];
    $out = [];
    foreach ($h as $k => $v) { $out[(string) $k] = is_array($v) ? ($v[0] ?? null) : $v; }
    return $out;
}

// ------------------------------------------------------------------ 0. versions
say('0. versions and environment');
info("bootstrap        $BOOTSTRAP");
info("runId            $RUN");
info('php              ' . PHP_VERSION . ' ' . PHP_OS . '/' . php_uname('m'));
info('php-rdkafka      ' . phpversion('rdkafka'));
ob_start(); phpinfo(INFO_MODULES); $pi = ob_get_clean();
foreach (['librdkafka version (runtime)', 'librdkafka version (build)'] as $needle) {
    if (preg_match('/' . preg_quote($needle, '/') . '\s*=>\s*(\S+)/', $pi, $m)) {
        info(sprintf('%-16s %s', trim(str_replace(['librdkafka version', '(', ')'], '', $needle)), $m[1]));
    }
}
info("protocol trace   $TRACE");
dump_conf('producer conf', ['bootstrap.servers' => $BOOTSTRAP, 'acks' => 'all', 'enable.idempotence' => 'false', 'debug' => 'protocol,msg']);
dump_conf('consumer conf', ['bootstrap.servers' => $BOOTSTRAP, 'group.id' => $GROUP_MAIN, 'auto.offset.reset' => 'earliest', 'enable.idempotence' => 'false']);

// ------------------------------------------------------------------ 1. auto-create
say('1. produce to a topic that does not exist yet (auto-create)');
// NOTE: it is librdkafka's own Metadata request for the unknown topic that trips
// the facade's auto-create, not the ProduceRequest. The user-visible contract is
// still the one asserted here.
[$reports, $errors] = produce_range($TOPIC_AUTO, [], 0, $PARTITIONS, $PARTITIONS);
check($errors === 0, "$PARTITIONS messages delivered to the never-before-seen topic $TOPIC_AUTO");
check(count($reports) === $PARTITIONS, count($reports) . "/$PARTITIONS delivery reports came back");
$parts = array_column($reports, 'partition'); sort($parts);
check($parts === range(0, $PARTITIONS - 1), "the broker confirmed one message on each of partitions 0..".($PARTITIONS-1));
check(array_unique(array_column($reports, 'offset')) === [0], 'every partition of a brand-new topic starts at offset 0');

$mdProducer = new RdKafka\Producer(producer_conf());
$md = $mdProducer->getMetadata(false, $mdProducer->newTopic($TOPIC_AUTO), 10000);
foreach ($md->getTopics() as $t) {
    if ($t->getTopic() === $TOPIC_AUTO) {
        check(count($t->getPartitions()) === $PARTITIONS,
              "auto-created topic reports " . count($t->getPartitions()) . " partitions (expected $PARTITIONS)");
    }
}

// ------------------------------------------------------------------ 2. bulk produce
say("2. bulk produce, UNCOMPRESSED: " . NMAIN . " messages over $PARTITIONS partitions, keys + headers");
$t0 = now();
[$reports, $errors] = produce_range($TOPIC_MAIN, [], 0, NMAIN, $PARTITIONS);
check($errors === 0, 'no delivery errors across ' . NMAIN . ' messages');
check(count($reports) === NMAIN, count($reports) . '/' . NMAIN . ' delivery reports');
$perPart = [];
foreach ($reports as $r) { $perPart[$r['partition']] = ($perPart[$r['partition']] ?? 0) + 1; }
ksort($perPart);
check(array_keys($perPart) === range(0, $PARTITIONS - 1),
      "messages landed on all $PARTITIONS partitions (>= 4 required): " . json_encode($perPart));
check(array_unique(array_values($perPart)) === [(int) (NMAIN / $PARTITIONS)],
      'each partition took exactly ' . (int) (NMAIN / $PARTITIONS));
// acks=all means these offsets came from the broker. Dense 0..63 per partition is
// also a statement that nothing was dropped.
$byPartOffsets = [];
foreach ($reports as $r) { $byPartOffsets[$r['partition']][] = $r['offset']; }
$dense = true;
foreach ($byPartOffsets as $p => $offs) { sort($offs); if ($offs !== range(0, (int)(NMAIN / $PARTITIONS) - 1)) { $dense = false; } }
check($dense, 'broker-assigned offsets are dense 0..' . ((int)(NMAIN / $PARTITIONS) - 1) . ' on every partition');
info(sprintf('produced %d msgs in %.2fs', NMAIN, now() - $t0));

// ------------------------------------------------------------------ 3. group consume
say('3. consume with a consumer GROUP (librdkafka stock defaults, auto-commit ON)');
$consumer = new RdKafka\KafkaConsumer(consumer_conf($GROUP_MAIN));
$consumer->subscribe([$TOPIC_MAIN]);
$t0 = now();
$msgs = drain($consumer, NMAIN, 90);
info(sprintf('drained %d msgs in %.2fs (includes the facade\'s group-join delay)', count($msgs), now() - $t0));
check(count($msgs) === NMAIN, count($msgs) . '/' . NMAIN . ' messages consumed');

// ------------------------------------------------------------------ 4. round trip
say('4. round trip: count, per-partition order, byte-exact key/payload/headers');
$byPart = [];
foreach ($msgs as $m) { $byPart[$m->partition][] = $m; }
ksort($byPart);
check(array_keys($byPart) === range(0, $PARTITIONS - 1), "all $PARTITIONS partitions were assigned and read");

$monotonic = true;
foreach ($byPart as $ms) {
    $offs = array_map(fn($m) => $m->offset, $ms);
    $sorted = $offs; sort($sorted);
    if ($offs !== $sorted || count(array_unique($offs)) !== count($offs)) { $monotonic = false; }
}
check($monotonic, 'offsets are strictly increasing within every partition');

// The real order assertion: partition p was produced indices p, p+P, p+2P... in
// that order, so the recovered index sequence must be exactly that.
$orderBroken = [];
foreach ($byPart as $p => $ms) {
    usort($ms, fn($a, $b) => $a->offset <=> $b->offset);
    $got  = array_map(fn($m) => index_of($m->payload), $ms);
    $want = array_values(array_filter(range(0, NMAIN - 1), fn($i) => $i % $PARTITIONS === $p));
    if ($got !== $want) { $orderBroken[] = $p; }
}
check($orderBroken === [], 'produced order is preserved per partition (indices ' . $PARTITIONS . '-strided)'
    . ($orderBroken === [] ? '' : ' -- broken on ' . json_encode($orderBroken)));

$seen = array_map(fn($m) => index_of($m->payload), $msgs);
sort($seen);
check($seen === range(0, NMAIN - 1), 'every index 0..' . (NMAIN - 1) . ' arrived exactly once, none invented');

$badPayload = 0;
foreach ($msgs as $m) { if ($m->payload !== payload_for(index_of($m->payload))) { $badPayload++; } }
check($badPayload === 0, "payloads are byte-exact including the 0x00..0xff blob ($badPayload mismatches)");

$badKey = 0;
foreach ($msgs as $m) { if ($m->key !== key_for(index_of($m->payload))) { $badKey++; } }
check($badKey === 0, "keys are byte-exact ($badKey mismatches)");

info('raw headers as the extension hands them back: ' . json_encode(($msgs[0]->headers ?? null)));
$badHdr = 0;
foreach ($msgs as $m) {
    $h = headers_of($m);
    $want = headers_for(index_of($m->payload));
    if (($h['idx'] ?? null) !== $want['idx'] || ($h['trace'] ?? null) !== $want['trace'] || ($h['uni'] ?? null) !== $want['uni']) { $badHdr++; }
}
check($badHdr === 0, "headers round-trip byte-exact, multibyte UTF-8 included ($badHdr mismatches)");
// An empty header value is not the same thing as a null one on the wire. Report
// what the extension hands back rather than asserting a preference.
$emptyVals = [];
foreach ($msgs as $m) { $emptyVals[var_export(headers_of($m)['empty'] ?? null, true)] = true; }
info('empty-string header value comes back as ' . implode('|', array_keys($emptyVals))
   . ' (Kafka distinguishes empty from null)');
check(count($emptyVals) === 1, 'the empty header value is consistent across all ' . NMAIN . ' messages');

$consumer->commit();
$consumer->close();

// ------------------------------------------------------------------ 5. compression
// Whether a codec is ACTUALLY applied depends on which librdkafka the packaging
// linked -- see probe_compression.php for the full diagnosis and the 2.11.1
// boundary. What must hold either way is that the records land and round-trip
// byte-exact, so that is what is asserted; whether the batch went out compressed is
// REPORTED from librdkafka's own log rather than assumed in the section title.
say('5. compression: lz4 and zstd');
[$reports, $errors] = produce_range($TOPIC_LZ4, ['compression.codec' => 'lz4'], 0, NCODEC, $PARTITIONS);
check($errors === 0 && count($reports) === NCODEC, count($reports) . '/' . NCODEC . ' messages produced with compression.codec=lz4');
[$reports, $errors] = produce_range($TOPIC_ZSTD, ['compression.codec' => 'zstd'], 0, NZSTD, $PARTITIONS);
check($errors === 0 && count($reports) === NZSTD, count($reports) . '/' . NZSTD . ' messages produced with compression.codec=zstd');

// librdkafka names the topic in its COMPRESSION notice, and each topic here carries
// its codec, so this attributes a downgrade to the right batch.
fflush($TRACE_FH);
$downgraded = function (string $topic) use ($TRACE): bool {
    foreach (file($TRACE, FILE_IGNORE_NEW_LINES) ?: [] as $l) {
        if (str_contains($l, 'does not support compression type') && str_contains($l, $topic)) { return true; }
    }
    return false;
};
$lz4Down  = $downgraded($TOPIC_LZ4);
$zstdDown = $downgraded($TOPIC_ZSTD);
info('lz4  batches went out ' . ($lz4Down  ? 'UNCOMPRESSED (librdkafka downgraded them)' : 'COMPRESSED'));
info('zstd batches went out ' . ($zstdDown ? 'UNCOMPRESSED (librdkafka downgraded them)' : 'COMPRESSED'));
check($zstdDown, 'zstd is downgraded, as documented: librdkafka gates zstd on Fetch v10 and the facade caps Fetch at v6 on purpose');
if ($lz4Down) {
    info('lz4 was downgraded TOO. That is NOT the Fetch cap -- librdkafka <= 2.11.0 tests');
    info('  ApiVersion_supported(Produce, 0, 0) for lz4/gzip/snappy, which returns -1 because');
    info('  the facade\'s Produce floor is v3. See probe_compression.php. Records still land.');
}

$codecConsumer = new RdKafka\KafkaConsumer(consumer_conf($GROUP_CODEC));
$codecConsumer->subscribe([$TOPIC_LZ4, $TOPIC_ZSTD]);
$codecMsgs = drain($codecConsumer, NCODEC + NZSTD, 120);
$lz4  = array_values(array_filter($codecMsgs, fn($m) => $m->topic_name === $TOPIC_LZ4));
$zstd = array_values(array_filter($codecMsgs, fn($m) => $m->topic_name === $TOPIC_ZSTD));
check(count($lz4) === NCODEC, count($lz4) . '/' . NCODEC . ' lz4-produced messages read back');
check(count($zstd) === NZSTD, count($zstd) . '/' . NZSTD . ' zstd-configured messages read back');
$lz4Bad = 0; foreach ($lz4 as $m) { if ($m->payload !== payload_for(index_of($m->payload))) { $lz4Bad++; } }
check($lz4Bad === 0, 'lz4-configured payloads are byte-exact' . ($lz4Down ? '' : ' after the facade decompressed them'));
$zBad = 0; foreach ($zstd as $m) { if ($m->payload !== payload_for(index_of($m->payload))) { $zBad++; } }
check($zBad === 0, 'zstd-configured payloads are byte-exact');
$hdrBad = 0; foreach ($lz4 as $m) { if ((headers_of($m)['uni'] ?? null) !== "h\xc3\xa9llo-\xc3\xbcn\xc3\xafcode") { $hdrBad++; } }
check($hdrBad === 0, 'headers survive a compressed batch');
$codecConsumer->close();

// ------------------------------------------------------------------ 6. watermarks
say('6. earliest/latest watermarks, committed offsets, and repositioning');
$probe = new RdKafka\KafkaConsumer(consumer_conf("$GROUP_MAIN-probe"));
$lows = []; $highs = [];
for ($p = 0; $p < $PARTITIONS; $p++) {
    $low = 0; $high = 0;
    $probe->queryWatermarkOffsets($TOPIC_MAIN, $p, $low, $high, 10000);
    $lows[] = $low; $highs[] = $high;
}
check(array_unique($lows) === [0], 'every partition\'s EARLIEST watermark is 0 (' . json_encode($lows) . ')');
check(array_unique($highs) === [(int) (NMAIN / $PARTITIONS)],
      'every partition\'s LATEST watermark is ' . (int)(NMAIN / $PARTITIONS) . ' (' . json_encode($highs) . ')');
check(array_sum($highs) - array_sum($lows) === NMAIN, 'watermarks account for exactly ' . NMAIN . ' messages');

// php-rdkafka 6.0.5 has NO KafkaConsumer::seek (the Ruby gem has both seek and
// seek_by). Repositioning is therefore assign() with an explicit offset, which
// issues the same Fetch. Read one, read on, reposition to the start, and require
// the identical record back -- that is what proves the reposition reached the
// facade rather than being served from a client-side buffer.
check(!method_exists('RdKafka\KafkaConsumer', 'seek'),
      'php-rdkafka ' . phpversion('rdkafka') . ' exposes no KafkaConsumer::seek -- using assign(offset) instead (CLIENT gap, not a facade gap)');
$probe->assign([new RdKafka\TopicPartition($TOPIC_MAIN, 0, 0)]);
$first = drain($probe, 1, 30);
if (count($first) === 0) {
    bad('assign(partition 0, offset 0) produced no message');
} else {
    $f = $first[0];
    ok("assign at an explicit offset returned index " . index_of($f->payload) . " at offset {$f->offset}");
    $skipped = drain($probe, 3, 30);
    $probe->assign([new RdKafka\TopicPartition($TOPIC_MAIN, 0, $f->offset)]);
    $again = drain($probe, 1, 30);
    check(count($again) === 1 && $again[0]->offset === $f->offset && $again[0]->payload === $f->payload,
          "re-assign back to offset {$f->offset} re-delivered the identical record after reading " . count($skipped) . " more");
}

$tps = [];
for ($p = 0; $p < $PARTITIONS; $p++) { $tps[] = new RdKafka\TopicPartition($TOPIC_MAIN, $p); }
$committedConsumer = new RdKafka\KafkaConsumer(consumer_conf($GROUP_MAIN));
$committed = $committedConsumer->getCommittedOffsets($tps, 15000);
$offs = array_map(fn($tp) => $tp->getOffset(), $committed);
info("committed offsets for $GROUP_MAIN: " . json_encode($offs));
check(array_sum(array_filter($offs, fn($o) => $o >= 0)) === NMAIN,
      'the group committed ' . array_sum(array_filter($offs, fn($o) => $o >= 0)) . ' of ' . NMAIN
      . ' (OffsetCommit + OffsetFetch round-trip)');
$committedConsumer->close();
$probe->close();

// ------------------------------------------------------------------ 7. resume
say('7. commit, stop, and resume in the SAME group from a NEW consumer instance');
$a = new RdKafka\KafkaConsumer(consumer_conf($GROUP_RESUME, ['enable.auto.commit' => 'false']));
$a->subscribe([$TOPIC_MAIN]);
$firstHalf = drain($a, RESUME_AT, 90);
check(count($firstHalf) === RESUME_AT, 'consumer A read ' . count($firstHalf) . '/' . RESUME_AT . ' before committing');
try { $a->commit(); ok('consumer A committed its positions synchronously'); }
catch (Throwable $e) { bad('consumer A commit threw ' . get_class($e) . ': ' . $e->getMessage()); }
$a->close();
info("consumer A closed; a NEW instance now joins $GROUP_RESUME");

$b = new RdKafka\KafkaConsumer(consumer_conf($GROUP_RESUME, ['enable.auto.commit' => 'false']));
$b->subscribe([$TOPIC_MAIN]);
$secondHalf = drain($b, NMAIN - RESUME_AT, 90);
$b->close();

$setA = array_map(fn($m) => index_of($m->payload), $firstHalf);
$setB = array_map(fn($m) => index_of($m->payload), $secondHalf);
$union = array_unique(array_merge($setA, $setB));
$dups  = array_intersect($setA, $setB);
check(count($union) === NMAIN, 'A + B together saw all ' . NMAIN . ' indices -- NO LOSS across the restart (saw ' . count($union) . ')');
check(count($secondHalf) >= NMAIN - RESUME_AT,
      'consumer B resumed and read the remaining ' . (NMAIN - RESUME_AT) . ' (got ' . count($secondHalf) . ')');
if (count($dups) === 0) {
    ok("zero duplicates: B started exactly where A's commit left off");
} else {
    info(count($dups) . ' indices were re-delivered to B (allowed: at-least-once redelivery of an uncommitted tail)');
    check(count($dups) < NMAIN / 4, 'redelivery is bounded (' . count($dups) . ' < ' . (int)(NMAIN / 4) . '), not a full rewind');
}

// ------------------------------------------------------------------ 8. SASL/TLS
// Optional lane, skipped entirely unless a SASL listener was named. The SASL
// PASSWORD is the Queen bearer token; the username is a free label the facade only
// logs. Verification stays ON unless KAFKA_SSL_INSECURE says otherwise -- the rig's
// self-signed cert has no host.docker.internal SAN, so a containerised client must
// be told to skip hostname verification or be handed the CA.
say('8. SASL/PLAIN over TLS');
$saslBootstrap = getenv('KAFKA_SASL_BOOTSTRAP') ?: '';
if ($saslBootstrap === '') {
    info('skipped: KAFKA_SASL_BOOTSTRAP is unset');
} else {
    $saslConf = [
        'bootstrap.servers' => $saslBootstrap,
        'security.protocol' => getenv('KAFKA_SASL_PROTOCOL') ?: 'sasl_ssl',
        'sasl.mechanisms'   => 'PLAIN',
        'sasl.username'     => getenv('KAFKA_SASL_USER') ?: 'phprdk',
        'sasl.password'     => getenv('KAFKA_SASL_TOKEN') ?: '',
    ];
    if (getenv('KAFKA_SSL_CA')) {
        $saslConf['ssl.ca.location'] = getenv('KAFKA_SSL_CA');
        $saslConf['ssl.endpoint.identification.algorithm'] = 'none';
    } elseif (getenv('KAFKA_SSL_INSECURE') === '1') {
        $saslConf['enable.ssl.certificate.verification'] = 'false';
    }
    dump_conf('sasl conf', array_merge($saslConf, ['sasl.password' => '<redacted>']));
    $topicSasl = "phprdk-sasl-$RUN";
    $groupSasl = "phprdk-gs-$RUN";
    [$reports, $errors] = produce_range($topicSasl, $saslConf, 0, $PARTITIONS, $PARTITIONS);
    check($errors === 0 && count($reports) === $PARTITIONS,
          count($reports) . "/$PARTITIONS messages produced over SASL_SSL (password = the Queen bearer token)");
    $sc = new RdKafka\KafkaConsumer(consumer_conf($groupSasl, $saslConf));
    $sc->subscribe([$topicSasl]);
    $saslMsgs = drain($sc, $PARTITIONS, 60);
    $sc->close();
    check(count($saslMsgs) === $PARTITIONS, count($saslMsgs) . "/$PARTITIONS messages consumed by a GROUP over SASL_SSL");
    $sBad = 0; foreach ($saslMsgs as $m) { if ($m->payload !== payload_for(index_of($m->payload))) { $sBad++; } }
    check($sBad === 0, 'SASL_SSL payloads are byte-exact');

    // A wrong password must be REFUSED, not silently accepted. Without this the
    // lane only proves TLS works, not that the credential is checked.
    if ((getenv('KAFKA_SASL_TOKEN') ?: '') !== '') {
        $refused = false;
        $wrong = producer_conf(array_merge($saslConf, ['sasl.password' => 'definitely-not-the-token']));
        $wrong->setErrorCb(function ($kafka, int $err, string $reason) use (&$refused) {
            if ($err === RD_KAFKA_RESP_ERR__AUTHENTICATION || $err === RD_KAFKA_RESP_ERR_SASL_AUTHENTICATION_FAILED
                || stripos($reason, 'authentic') !== false || stripos($reason, '401') !== false) {
                if (!$refused) { ok('a wrong SASL password is refused: ' . substr($reason, 0, 200)); }
                $refused = true;
            }
        });
        $wp = new RdKafka\Producer($wrong);
        $wt = $wp->newTopic($topicSasl);
        $wt->producev(0, RD_KAFKA_MSG_F_BLOCK, 'nope', 'nope', []);
        $stop = now() + 20;
        while (!$refused && now() < $stop) { $wp->poll(200); }
        check($refused, 'the facade refused a wrong SASL credential rather than accepting it');
    }
}

// ------------------------------------------------------------------ 9. versions
say('9. API versions this client actually NEGOTIATED (read from librdkafka debug=protocol)');
fflush($TRACE_FH);
$sent = [];
$codecNotes = [];
foreach (file($TRACE, FILE_IGNORE_NEW_LINES) ?: [] as $line) {
    if (preg_match('/Sent (\w+?)Request \(v(\d+)/', $line, $m)) { $sent[$m[1]][] = (int) $m[2]; }
    // librdkafka's own CODEC notice. It rides the MSG debug facility, which is
    // why base_conf sets `debug=protocol,msg` and not just protocol. It is also
    // rate limited to once per broker HANDLE, which is why section 5 gives each
    // codec its own producer. Matched narrowly: the startup banner lists ZSTD
    // among builtin.features and would otherwise be picked up instead.
    if (str_contains($line, 'does not support compression type')) { $codecNotes[] = $line; }
}
if ($sent === []) {
    bad("librdkafka's debug=protocol stream produced no 'Sent xRequest' lines -- cannot report negotiated versions");
} else {
    ksort($sent);
    foreach ($sent as $k => $vs) { $vs = array_values(array_unique($vs)); sort($vs); $sent[$k] = $vs; info(sprintf('%-18s v%s', $k, implode(',', $vs))); }
    check(isset($sent['ApiVersion']), 'the connection began with ApiVersions, so every version below was negotiated and not assumed');
    check(isset($sent['Fetch']) && max($sent['Fetch']) <= 6,
          'Fetch negotiated down to v' . (isset($sent['Fetch']) ? max($sent['Fetch']) : '?') . ' (facade caps at 6 on purpose: v7 is fetch sessions)');
    check(isset($sent['Produce']) && max($sent['Produce']) <= 9,
          'Produce negotiated to v' . (isset($sent['Produce']) ? max($sent['Produce']) : '?'));
    check(isset($sent['JoinGroup'], $sent['SyncGroup'], $sent['Heartbeat']),
          'the full group handshake ran: JoinGroup v' . implode(',', $sent['JoinGroup'] ?? [])
          . ', SyncGroup v' . implode(',', $sent['SyncGroup'] ?? [])
          . ', Heartbeat v' . implode(',', $sent['Heartbeat'] ?? []));
    check(!isset($sent['InitProducerId']), 'the client never attempted InitProducerId (idempotence stayed off)');
}
// FAILS CLOSED, and it did not used to. This was `if ($codecNotes === []) {
// info(...) } else { check(...) }`: an EMPTY signal printed a note and the run
// passed, so the one outcome that must never pass quietly -- the detector going
// blind -- was exactly the outcome that did. A compression regression hides in
// that branch: whether the facade started answering Fetch v10, or librdkafka
// reworded its notice, or the trace file stopped being written, the visible
// effect is the same missing line, and none of the three is something to shrug
// at.
//
// The downgrade is not optional here. librdkafka gates zstd PRODUCE on the
// broker advertising Fetch v10; queen-kafka caps Fetch at v6 on purpose
// (versions.rs -- v7 is fetch sessions), so EVERY run of section 5 must produce
// this notice. Its absence is a result, not a silence.
foreach (array_unique($codecNotes) as $l) { info('librdkafka CODEC: ' . trim($l)); }
$z = false; foreach ($codecNotes as $l) { if (stripos($l, 'zstd') !== false) { $z = true; } }
check($z, 'librdkafka itself reported the zstd downgrade (' . count($codecNotes) . ' codec notice(s)) -- the records in section 5 landed UNCOMPRESSED, which is the Fetch-v6 cap working as designed, not a defect');

// ------------------------------------------------------------------ done
echo "\n";
$tag = 'php-rdkafka ' . phpversion('rdkafka');
if ($FAIL === 0) { echo "RESULT: PASS ($tag)\n"; exit(0); }
echo "RESULT: FAIL ($FAIL) ($tag)\n";
exit(1);
