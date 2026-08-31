<?php
/**
 * Why every librdkafka client sends UNCOMPRESSED batches to queen-kafka.
 *
 *   php probe-compression.php [bootstrap]
 *
 * compat/README.md documents ONE compression downgrade -- zstd, because librdkafka
 * gates the zstd feature on Fetch v10 and the facade caps Fetch at v6 on purpose.
 * That is real, but it is not the whole story, and this probe exists to show the
 * rest of it: gzip, snappy and lz4 are downgraded too, for a DIFFERENT reason that
 * has nothing to do with Fetch.
 *
 * librdkafka, rdkafka_msgset_writer.c, rd_kafka_msgset_writer_select_MsgVersion():
 *
 *     static const struct { int feature; int16_t ApiVersion; }
 *     compr_req[RD_KAFKA_COMPRESSION_NUM] = {
 *             [RD_KAFKA_COMPRESSION_LZ4]  = {RD_KAFKA_FEATURE_LZ4,  0},
 *             [RD_KAFKA_COMPRESSION_ZSTD] = {RD_KAFKA_FEATURE_ZSTD, 7},
 *     };
 *     if (msetw->msetw_compression &&
 *         (rd_kafka_broker_ApiVersion_supported(
 *              rkb, RD_KAFKAP_Produce, 0,
 *              compr_req[msetw->msetw_compression].ApiVersion, NULL) == -1 || ...))
 *             msetw->msetw_compression = RD_KAFKA_COMPRESSION_NONE;
 *
 * GZIP and SNAPPY are absent from that initialiser, so their entries are zeroed:
 * {feature = 0, ApiVersion = 0}. LZ4's ApiVersion is 0 as well. All three therefore
 * ask the same question -- "does this broker support Produce somewhere in [0, 0]?"
 * -- and rd_kafka_broker_ApiVersion_supported() answers it like this:
 *
 *     else if (ret.MinVer > maxver)  return -1;
 *
 * queen-kafka advertises Produce 3..=9 (versions.rs). MinVer 3 > maxver 0, so the
 * answer is -1 and the codec is dropped. It is the Produce FLOOR that disables
 * gzip/snappy/lz4, not any missing feature -- against a real broker, which
 * advertises Produce from v0, the same call returns 0 and all three compress.
 *
 * zstd is the only one that clears the ApiVersion hurdle (maxver 7 overlaps 3..9)
 * and then fails the SECOND test, the feature bit, which needs Fetch v10.
 *
 * The consequence is invisible in a test that only checks records land: they do
 * land, byte-exact, and librdkafka says so once per DAY per broker at LOG_NOTICE
 * (rd_interval(..., 86400 * 1000 * 1000, 0)). What you lose is the compression.
 */

declare(strict_types=1);

$bootstrap = $argv[1] ?? '127.0.0.1:19092';
$run       = $argv[2] ?? (string) time();

// A payload that any codec would crush: 4 KB of one byte. If a batch really were
// compressed, the produced request would be a fraction of the raw size.
$payload = str_repeat('A', 4096);
$fail    = 0;

printf("bootstrap  %s\n", $bootstrap);
printf("librdkafka %s (php-rdkafka %s)\n\n", (function () {
    ob_start(); phpinfo(INFO_MODULES); $pi = ob_get_clean();
    return preg_match('/librdkafka version \(runtime\)\s*=>\s*(\S+)/', $pi, $m) ? $m[1] : '?';
})(), phpversion('rdkafka'));

foreach (['none', 'gzip', 'snappy', 'lz4', 'zstd'] as $codec) {
    $downgraded = null;
    $conf = new RdKafka\Conf();
    $conf->set('bootstrap.servers', $bootstrap);
    $conf->set('enable.idempotence', 'false');
    $conf->set('acks', 'all');
    $conf->set('compression.codec', $codec);
    $conf->set('debug', 'msg,protocol');
    $conf->set('log_level', '7');
    // The LOG_NOTICE is rate-limited to once per day PER BROKER HANDLE, so each
    // codec gets a brand-new producer or only the first would ever report.
    $conf->setLogCb(function ($k, $l, $f, $m) use (&$downgraded) {
        if (str_contains($m, 'does not support compression type')) {
            $downgraded = trim(substr($m, (int) strpos($m, 'Broker does not support')));
        }
    });
    $delivered = 0;
    $conf->setDrMsgCb(function ($k, RdKafka\Message $m) use (&$delivered) { if (!$m->err) { $delivered++; } });

    $p = new RdKafka\Producer($conf);
    $t = $p->newTopic("codec-probe-$codec-$run");
    for ($i = 0; $i < 32; $i++) { $t->producev(0, RD_KAFKA_MSG_F_BLOCK, $payload, "k$i", []); $p->poll(0); }
    $p->flush(15000);

    $verdict = $codec === 'none'
        ? 'n/a'
        : ($downgraded === null ? 'COMPRESSED' : 'DOWNGRADED to none');
    printf("  %-7s delivered=%-3d %s\n", $codec, $delivered, $verdict);
    if ($downgraded !== null) { printf("          librdkafka: %s\n", $downgraded); }
    if ($delivered !== 32) { $fail++; printf("          FAIL only %d/32 delivered\n", $delivered); }
}

printf("\nEvery codec still DELIVERS -- this costs bandwidth, not correctness.\n");
exit($fail === 0 ? 0 : 1);
