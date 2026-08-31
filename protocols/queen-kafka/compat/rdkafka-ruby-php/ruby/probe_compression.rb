#!/usr/bin/env ruby
#
# The other side of the librdkafka compression boundary. Run it next to
# php/probe_compression.php and read the two together.
#
#   ruby probe_compression.rb [bootstrap] [runId]
#
# THE FINDING, IN ONE PARAGRAPH
#
# queen-kafka advertises Produce 3..=9 (versions.rs: v0-v2 are the legacy message
# sets nothing sends any more). Real Kafka advertises Produce from v0. librdkafka
# up to and including 2.11.0 decided whether a codec was usable like this
# (rdkafka_msgset_writer.c, rd_kafka_msgset_writer_select_MsgVersion):
#
#     compr_req[LZ4]  = {RD_KAFKA_FEATURE_LZ4,  0};
#     compr_req[ZSTD] = {RD_KAFKA_FEATURE_ZSTD, 7};
#     /* gzip and snappy are absent, so their entries are zeroed: {0, 0} */
#     if (rd_kafka_broker_ApiVersion_supported(rkb, RD_KAFKAP_Produce,
#                                              0, compr_req[codec].ApiVersion,
#                                              NULL) == -1 || ...)
#             msetw->msetw_compression = RD_KAFKA_COMPRESSION_NONE;
#
# For gzip, snappy and lz4 that asks "does the broker support Produce somewhere in
# [0,0]?", and rd_kafka_broker_ApiVersion_supported() answers `if (ret.MinVer >
# maxver) return -1;`. MinVer 3 > 0, so all three codecs are silently dropped --
# not because of any missing feature, purely because of the Produce FLOOR.
#
# librdkafka 2.11.1 replaced that call with rd_kafka_broker_ApiVersion_at_least(),
# which asks the sane question ("is MaxVer >= N?"), and the problem disappears.
#
# So the SAME facade gives two different answers depending on which librdkafka the
# client's packaging happened to link:
#
#   * librdkafka <= 2.11.0  -- gzip, snappy, lz4 AND zstd all downgraded to none.
#   * librdkafka >= 2.11.1  -- gzip, snappy, lz4 compress; only zstd is downgraded,
#                             and that one IS the documented Fetch-v6 cap.
#
# The rdkafka gem ships a PRECOMPILED librdkafka and is currently on the new side.
# php-rdkafka links the SYSTEM librdkafka, and Debian bookworm's is 1.9.2 -- the old
# side. Neither is a correctness problem: every codec still DELIVERS, byte-exact,
# and librdkafka mentions it once per DAY per broker at LOG_NOTICE. What is lost is
# the compression, silently, on exactly the deployments that asked for it.

require "rdkafka"
require "logger"
require "stringio"

BOOTSTRAP = ARGV[0] || "127.0.0.1:19092"
RUN       = ARGV[1] || Time.now.to_i.to_s

# A payload any codec would crush: 4 KB of one byte.
PAYLOAD = ("A" * 4096).freeze

log = StringIO.new
Rdkafka::Config.logger = Logger.new(log)
Rdkafka::Config.logger.level = Logger::DEBUG

puts "bootstrap  #{BOOTSTRAP}"
puts "librdkafka #{Rdkafka::LIBRDKAFKA_VERSION} (rdkafka gem #{Rdkafka::VERSION})"
puts

fail_count = 0
%w[none gzip snappy lz4 zstd].each do |codec|
  mark = log.string.bytesize
  producer = Rdkafka::Config.new(
    "bootstrap.servers"  => BOOTSTRAP,
    "client.id"          => "codec-#{codec}-#{RUN}",
    "enable.idempotence" => "false",
    "acks"               => "all",
    "compression.codec"  => codec,
    "debug"              => "msg",
    "log_level"          => "7"
  ).producer

  handles = (0...32).map do |i|
    producer.produce(topic: "codec-probe-#{codec}-#{RUN}", payload: PAYLOAD, key: "k#{i}", partition: 0)
  end
  producer.flush(15_000)
  # rdkafka 0.29 renamed the seconds-valued `max_wait_timeout:` to
  # `max_wait_timeout_ms:`; try the new name and fall back for older gems.
  delivered = handles.count do |h|
    begin
      h.wait(max_wait_timeout_ms: 20_000)
    rescue ArgumentError
      h.wait(max_wait_timeout: 20)
    rescue StandardError
      nil
    end
  end
  producer.close
  sleep 0.3 # the gem drains its log queue on a background thread

  # The LOG_NOTICE is rate-limited to once per day PER BROKER HANDLE, which is why
  # every codec gets a brand-new producer here -- share one and only the first
  # codec would ever report.
  note = log.string.byteslice(mark..)&.lines&.find { |l| l.include?("does not support compression type") }
  verdict = codec == "none" ? "n/a" : (note ? "DOWNGRADED to none" : "COMPRESSED")
  puts format("  %-7s delivered=%-3d %s", codec, delivered, verdict)
  puts "          librdkafka: #{note.strip.sub(/\A.*?Broker does not/, 'Broker does not')}" if note
  if delivered != 32
    fail_count += 1
    puts "          FAIL only #{delivered}/32 delivered"
  end
end

puts
puts "Every codec still DELIVERS -- this costs bandwidth, not correctness."
exit(fail_count.zero? ? 0 : 1)
