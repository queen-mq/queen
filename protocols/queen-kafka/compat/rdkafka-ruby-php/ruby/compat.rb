#!/usr/bin/env ruby
#
# The `rdkafka` gem -- karafka's core -- against the queen-kafka facade.
#
#   ruby compat.rb [bootstrap] [runId]
#
# WHAT THIS PROVES, AND WHAT IT DOES NOT
#
# The librdkafka C core underneath this gem is already covered by compat/librdkafka.
# Re-proving the wire protocol here would be theatre. What is NOT covered anywhere
# else, and what this file is actually for, is the RUBY PACKAGING and the gem's own
# defaults:
#
#   * which librdkafka a plain `gem install rdkafka` actually lands you on (0.29.0
#     ships a PRECOMPILED aarch64-linux platform gem -- nothing is built from
#     source, so the C core's feature set is whatever the gem author linked, not
#     whatever dev headers you installed),
#   * whether the gem's Ruby-side surface (Producer#produce keyword args,
#     Consumer#poll, #seek_by, #query_watermark_offsets, #committed) round-trips
#     bytes intact through the facade,
#   * whether librdkafka's STOCK defaults -- which the gem does not override --
#     are safe here. They are, with one exception that is the facade's documented
#     surface rather than a bug: see the zstd section.
#
# BEHAVIOURS THAT ARE THE CLIENT'S FAULT, NOT THE FACADE'S
#
#   * zstd. librdkafka gates the zstd codec on Fetch v10; queen-kafka caps Fetch at
#     v6 on purpose (v7 is fetch sessions, KIP-227). So librdkafka decides the
#     broker "does not support" zstd and produces the batch UNCOMPRESSED instead of
#     failing. Records still land and still round-trip byte-exact. Section 5 asserts
#     exactly that and prints librdkafka's own line saying it downgraded.
#   * enable.idempotence. Advertised and enforced since M7 F3, so nothing here
#     needs it OFF any more; librdkafka's default is off and it is set
#     explicitly anyway -- a run should not depend on a default staying put.
#     The idempotent path itself is measured in compat/rust-rdkafka, on the same
#     librdkafka.
#   * The gem exposes headers as a plain Hash, so a repeated header key cannot be
#     represented; see section 4's note.
#
# Every blocking call has a deadline. A hang is a result, and a suite that waits
# forever reports nothing.

require "rdkafka"
require "logger"
require "set"
require "tmpdir"

BOOTSTRAP = ARGV[0] || "127.0.0.1:19092"
RUN       = ARGV[1] || Time.now.to_i.to_s

TOPIC_MAIN   = "rbrdk-#{RUN}"
TOPIC_LZ4    = "rbrdk-lz4-#{RUN}"
TOPIC_ZSTD   = "rbrdk-zstd-#{RUN}"
TOPIC_AUTO   = "rbrdk-auto-#{RUN}"
GROUP_MAIN   = "rbrdk-g-#{RUN}"
GROUP_CODEC  = "rbrdk-gc-#{RUN}"
GROUP_RESUME = "rbrdk-gr-#{RUN}"

PARTITIONS = Integer(ENV.fetch("KAFKA_PARTITIONS", "8"))
NMAIN      = 512                  # the bar: >= 500 across >= 4 partitions
NCODEC     = 512
NZSTD      = 128
RESUME_AT  = 200

TRACE = ENV["NEGOTIATED_TRACE_FILE"] || File.join(Dir.tmpdir, "rbrdk-#{RUN}.trace.log")

# ---------------------------------------------------------------- reporting
$fail = 0
def say(s)  = puts("\n=== #{s}")
def ok(s)   = puts("  ok   #{s}")
def info(s) = puts("  ..   #{s}")

def bad(s)
  $fail += 1
  puts "  FAIL #{s}"
end

def check(cond, s)
  cond ? ok(s) : bad(s)
  cond
end

def now  = Process.clock_gettime(Process::CLOCK_MONOTONIC)
def dl(s) = now + s

# rdkafka 0.29 deprecated the seconds-valued `max_wait_timeout:` in favour of
# `max_wait_timeout_ms:`, and older gems only know the former. Try the new name and
# fall back, so this suite runs against whatever `gem install rdkafka` resolves to
# without printing a deprecation banner per message.
def wait_handle(handle, ms)
  handle.wait(max_wait_timeout_ms: ms)
rescue ArgumentError
  handle.wait(max_wait_timeout: ms / 1000)
end

# ---------------------------------------------------------------- fixture
# 256 raw bytes, every value 0x00..0xff, so "byte-exact round trip" means what it
# says: NULs, high bytes, and anything a UTF-8 assumption would mangle. It is also
# identical in every message, which makes the payload highly compressible -- that
# is what section 5 needs to tell a real lz4 batch from a downgraded one.
BLOB = (0..255).map(&:chr).join.force_encoding(Encoding::BINARY).freeze

def payload_for(i)
  (format("idx=%06d;", i) + BLOB + format(";fin=%06d", i)).force_encoding(Encoding::BINARY)
end

def key_for(i) = format("k-%06d", i)

def headers_for(i)
  {
    "idx"   => format("%06d", i),
    "trace" => "rbrdk-#{RUN}",
    "uni"   => "héllo-ünïcode",   # multibyte UTF-8
    "empty" => ""                                # empty != null; see section 4
  }
end

# Every message carries its own index in the payload, so a consumer can rebuild
# what was produced without trusting order, and per-partition order is then a
# property of the recovered index sequence rather than of the loop that produced it.
def index_of(payload)
  m = payload.to_s.byteslice(0, 16).match(/idx=(\d{6});/)
  m && Integer(m[1], 10)
end

# The gem has returned header keys as Symbols in some releases and Strings in
# others, and newer librdkafka can hand back an Array for a repeated key. Normalise
# both so the assertions test the FACADE and not the gem's changelog -- and print
# what the raw shape actually was (section 4) so the difference stays visible.
def hdr(msg)
  (msg.headers || {}).each_with_object({}) do |(k, v), acc|
    acc[k.to_s] = v.is_a?(Array) ? v.first : v
  end
end

# ---------------------------------------------------------------- config
def base_conf(extra = {})
  {
    "bootstrap.servers" => BOOTSTRAP,
    "client.id"         => "rbrdk-#{RUN}",
    # librdkafka defaults this off and this suite keeps it off on purpose; say
    # it anyway so the suite does not silently depend on that default. Since
    # M7 F3 the facade would accept it on.
    "enable.idempotence" => "false",
    # Read the negotiated API versions out of librdkafka's own mouth rather than
    # assuming them (compat/ convention 5).
    # `protocol` AND `msg`. protocol is what section 9 reads the negotiated
    # versions out of. msg is NOT optional decoration: librdkafka's
    # "Broker does not support compression type X" line is emitted through the
    # MSG debug facility, so with `debug=protocol` alone the compression
    # downgrade is INVISIBLE to this suite -- measured, both facilities side by
    # side against the same facade: protocol=no notice, msg=notice seen. That
    # blindness is what section 5 and section 9 assert against.
    "debug"     => "protocol,msg",
    "log_level" => "7"
  }.merge(extra)
end

def producer_conf(extra = {}) = base_conf({ "acks" => "all" }.merge(extra))

# NOTE: the consumer conf deliberately leaves session.timeout.ms,
# heartbeat.interval.ms and partition.assignment.strategy at librdkafka's stock
# values. Testing the DEFAULTS is the point of this row of the matrix: a user who
# writes four lines of Ruby gets exactly this.
def consumer_conf(group, extra = {})
  base_conf({ "group.id" => group, "auto.offset.reset" => "earliest" }.merge(extra))
end

def dump_conf(label, h)
  info("#{label}: " + h.map { |k, v| "#{k}=#{v}" }.sort.join(" "))
end

# ---------------------------------------------------------------- helpers
# Produce a range of indices, waiting on every delivery report. Returns the
# reports keyed by index so the caller can assert on real broker-assigned offsets
# (acks=all, so these are the broker's numbers and not the client's guesses).
def produce_range(producer, topic, range, partitions)
  handles = {}
  range.each do |i|
    handles[i] = producer.produce(
      topic:     topic,
      payload:   payload_for(i),
      key:       key_for(i),
      partition: i % partitions,
      headers:   headers_for(i)
    )
  end
  producer.flush(30_000)
  reports = {}
  errors  = 0
  handles.each do |i, h|
    begin
      reports[i] = wait_handle(h, 30_000)
    rescue StandardError => e
      errors += 1
      bad("delivery report for #{i} raised #{e.class}: #{e.message}") if errors <= 3
    end
  end
  [reports, errors]
end

# Poll until `want` messages arrive or the deadline passes. Returns the messages.
# A short deadline here is what turns a facade hang into a FAIL line instead of a
# suite that never returns.
def drain(consumer, want, secs)
  got  = []
  stop = dl(secs)
  while got.size < want && now < stop
    begin
      m = consumer.poll(1000)
    rescue Rdkafka::RdkafkaError => e
      bad("poll raised #{e.code}: #{e.message}")
      break
    end
    got << m if m
  end
  got
end

# ---------------------------------------------------------------- start
trace_io = File.open(TRACE, "w")
trace_io.sync = true
Rdkafka::Config.logger = Logger.new(trace_io)
Rdkafka::Config.logger.level = Logger::DEBUG

say "0. versions and environment"
info "bootstrap        #{BOOTSTRAP}"
info "runId            #{RUN}"
info "ruby             #{RUBY_VERSION} #{RUBY_PLATFORM}"
info "gem rdkafka      #{Rdkafka::VERSION}"
info "librdkafka       #{Rdkafka::LIBRDKAFKA_VERSION}"
info "gem platform     #{Gem.loaded_specs['rdkafka']&.platform} (a NON-'ruby' platform here means a PRECOMPILED librdkafka shipped in the gem; nothing was built from source)"
info "protocol trace   #{TRACE}"
dump_conf("producer conf", producer_conf)
dump_conf("consumer conf", consumer_conf(GROUP_MAIN))

# ---------------------------------------------------------------- 1. auto-create
say "1. produce to a topic that does not exist yet (auto-create)"
# NOTE: it is librdkafka's own Metadata request for the unknown topic that trips
# the facade's auto-create, not the ProduceRequest -- a bare metadata lookup is
# enough (see compat/README.md). Either way the user-visible contract is the one
# asserted here: produce to a never-seen topic succeeds and the topic exists at
# the facade's configured width.
auto_producer = Rdkafka::Config.new(producer_conf).producer
reports, errors = produce_range(auto_producer, TOPIC_AUTO, 0...PARTITIONS, PARTITIONS)
check(errors.zero?, "#{PARTITIONS} messages delivered to the never-before-seen topic #{TOPIC_AUTO}")
check(reports.size == PARTITIONS, "#{reports.size}/#{PARTITIONS} delivery reports came back")
pc = auto_producer.partition_count(TOPIC_AUTO)
check(pc == PARTITIONS, "auto-created topic reports #{pc} partitions (expected #{PARTITIONS})")
check(reports.values.map(&:partition).sort == (0...PARTITIONS).to_a,
      "the broker confirmed one message on each of partitions 0..#{PARTITIONS - 1}")
check(reports.values.map(&:offset).uniq == [0],
      "every partition of a brand-new topic starts at offset 0")
auto_producer.close

# ---------------------------------------------------------------- 2. bulk produce
say "2. bulk produce, UNCOMPRESSED: #{NMAIN} messages over #{PARTITIONS} partitions, keys + headers"
producer = Rdkafka::Config.new(producer_conf).producer
t0 = now
reports, errors = produce_range(producer, TOPIC_MAIN, 0...NMAIN, PARTITIONS)
check(errors.zero?, "no delivery errors across #{NMAIN} messages")
check(reports.size == NMAIN, "#{reports.size}/#{NMAIN} delivery reports")
per_part = reports.values.group_by(&:partition).transform_values(&:size)
check(per_part.keys.sort == (0...PARTITIONS).to_a,
      "messages landed on all #{PARTITIONS} partitions (>= 4 required): #{per_part.sort.to_h}")
check(per_part.values.uniq == [NMAIN / PARTITIONS],
      "each partition took exactly #{NMAIN / PARTITIONS}")
# acks=all means these offsets came from the broker. Assert they are the dense
# 0..63 range per partition, which is also a statement that nothing was dropped.
dense = reports.values.group_by(&:partition).all? { |_p, rs| rs.map(&:offset).sort == (0...(NMAIN / PARTITIONS)).to_a }
check(dense, "broker-assigned offsets are dense 0..#{NMAIN / PARTITIONS - 1} on every partition")
info format("produced %d msgs in %.2fs", NMAIN, now - t0)

# ---------------------------------------------------------------- 3. group consume
say "3. consume with a consumer GROUP (librdkafka stock defaults, auto-commit ON)"
consumer = Rdkafka::Config.new(consumer_conf(GROUP_MAIN)).consumer
consumer.subscribe(TOPIC_MAIN)
t0 = now
msgs = drain(consumer, NMAIN, 90)
info format("drained %d msgs in %.2fs (includes the facade's %sms group-join delay)",
            msgs.size, now - t0, ENV.fetch("KAFKA_JOIN_DELAY_MS", "3000"))
check(msgs.size == NMAIN, "#{msgs.size}/#{NMAIN} messages consumed")

say "4. round trip: count, per-partition order, byte-exact key/payload/headers"
by_part = msgs.group_by(&:partition)
check(by_part.keys.sort == (0...PARTITIONS).to_a, "all #{PARTITIONS} partitions were assigned and read")

offsets_monotonic = by_part.all? { |_p, ms| ms.map(&:offset) == ms.map(&:offset).sort && ms.map(&:offset).uniq.size == ms.size }
check(offsets_monotonic, "offsets are strictly increasing within every partition")

# The real order assertion: partition p was produced indices p, p+P, p+2P... in
# that order, so the recovered index sequence must be exactly that.
order_bad = by_part.reject do |p, ms|
  ms.sort_by(&:offset).map { |m| index_of(m.payload) } == (0...NMAIN).select { |i| i % PARTITIONS == p }
end
check(order_bad.empty?, "produced order is preserved per partition (indices #{PARTITIONS}-strided)#{order_bad.empty? ? '' : " -- broken on #{order_bad.keys.inspect}"}")

seen      = msgs.map { |m| index_of(m.payload) }
check(seen.compact.sort == (0...NMAIN).to_a, "every index 0..#{NMAIN - 1} arrived exactly once, none invented")

bad_payload = msgs.reject { |m| m.payload.b == payload_for(index_of(m.payload)).b }
check(bad_payload.empty?, "payloads are byte-exact including the 0x00..0xff blob (#{bad_payload.size} mismatches)")

bad_key = msgs.reject { |m| m.key == key_for(index_of(m.payload)) }
check(bad_key.empty?, "keys are byte-exact (#{bad_key.size} mismatches)")

info "raw headers as the gem hands them back: #{msgs.first.headers.inspect}"
bad_hdr = msgs.reject do |m|
  h = hdr(m)
  want = headers_for(index_of(m.payload))
  h["idx"] == want["idx"] && h["trace"] == want["trace"] && h["uni"].to_s.b == want["uni"].b
end
check(bad_hdr.empty?, "headers round-trip byte-exact, multibyte UTF-8 included (#{bad_hdr.size} mismatches)")
# An empty header value is not the same thing as a null one on the wire. Report
# what the gem hands back rather than asserting a preference.
empty_vals = msgs.map { |m| hdr(m)["empty"] }.uniq
info "empty-string header value comes back as #{empty_vals.inspect} (Kafka distinguishes empty from null; the gem's Hash cannot represent a REPEATED key at all)"
check(empty_vals.size == 1 && (empty_vals.first == "" || empty_vals.first.nil?),
      "the empty header value is consistent across all #{NMAIN} messages")

consumer.commit(nil, false) rescue nil
consumer.close

# ---------------------------------------------------------------- 5. compression
# Whether a codec is ACTUALLY applied depends on which librdkafka the packaging
# linked -- see probe_compression.rb for the full diagnosis and the 2.11.1
# boundary. What must hold either way is that the records land and round-trip
# byte-exact, so that is what is asserted; whether the batch went out compressed is
# REPORTED from librdkafka's own log rather than assumed in the section title.
say "5. compression: lz4 and zstd"

# librdkafka names the topic in its COMPRESSION notice, and each topic here carries
# its codec, so this attributes a downgrade to the right batch.
def downgraded?(trace_path, topic)
  File.foreach(trace_path).any? { |l| l.include?("does not support compression type") && l.include?(topic) }
rescue StandardError
  false
end

lz4_producer = Rdkafka::Config.new(producer_conf("compression.codec" => "lz4")).producer
reports, errors = produce_range(lz4_producer, TOPIC_LZ4, 0...NCODEC, PARTITIONS)
check(errors.zero? && reports.size == NCODEC, "#{reports.size}/#{NCODEC} messages produced with compression.codec=lz4")
lz4_producer.close

zstd_producer = Rdkafka::Config.new(producer_conf("compression.codec" => "zstd")).producer
reports, errors = produce_range(zstd_producer, TOPIC_ZSTD, 0...NZSTD, PARTITIONS)
check(errors.zero? && reports.size == NZSTD, "#{reports.size}/#{NZSTD} messages produced with compression.codec=zstd")
zstd_producer.close

trace_io.flush
sleep 0.5
lz4_down  = downgraded?(TRACE, TOPIC_LZ4)
zstd_down = downgraded?(TRACE, TOPIC_ZSTD)
info "lz4  batches went out #{lz4_down  ? 'UNCOMPRESSED (librdkafka downgraded them)' : 'COMPRESSED'}"
info "zstd batches went out #{zstd_down ? 'UNCOMPRESSED (librdkafka downgraded them)' : 'COMPRESSED'}"
check(zstd_down, "zstd is downgraded, as documented: librdkafka gates zstd on Fetch v10 and the facade caps Fetch at v6 on purpose")
if lz4_down
  info "lz4 was downgraded TOO. That is NOT the Fetch cap -- librdkafka <= 2.11.0 tests"
  info "  ApiVersion_supported(Produce, 0, 0) for lz4/gzip/snappy, which returns -1 because"
  info "  the facade's Produce floor is v3. See probe_compression.rb. Records still land."
end

codec_consumer = Rdkafka::Config.new(consumer_conf(GROUP_CODEC)).consumer
codec_consumer.subscribe(TOPIC_LZ4, TOPIC_ZSTD)
codec_msgs = drain(codec_consumer, NCODEC + NZSTD, 120)
lz4_msgs  = codec_msgs.select { |m| m.topic == TOPIC_LZ4 }
zstd_msgs = codec_msgs.select { |m| m.topic == TOPIC_ZSTD }
check(lz4_msgs.size == NCODEC, "#{lz4_msgs.size}/#{NCODEC} lz4-produced messages read back")
check(zstd_msgs.size == NZSTD, "#{zstd_msgs.size}/#{NZSTD} zstd-configured messages read back")
check(lz4_msgs.all? { |m| m.payload.b == payload_for(index_of(m.payload)).b },
      "lz4-configured payloads are byte-exact#{lz4_down ? '' : ' after the facade decompressed them'}")
check(zstd_msgs.all? { |m| m.payload.b == payload_for(index_of(m.payload)).b },
      "zstd-configured payloads are byte-exact")
check(lz4_msgs.all? { |m| hdr(m)["uni"].to_s.b == "héllo-ünïcode".b },
      "headers survive a compressed batch")
codec_consumer.close

# ---------------------------------------------------------------- 6. watermarks + seek
say "6. earliest/latest watermarks, committed offsets, and seek"
probe = Rdkafka::Config.new(consumer_conf("#{GROUP_MAIN}-probe")).consumer
lows, highs = [], []
(0...PARTITIONS).each do |p|
  low, high = probe.query_watermark_offsets(TOPIC_MAIN, p, 10_000)
  lows << low
  highs << high
end
check(lows.uniq == [0], "every partition's EARLIEST watermark is 0 (#{lows.inspect})")
check(highs.uniq == [NMAIN / PARTITIONS], "every partition's LATEST watermark is #{NMAIN / PARTITIONS} (#{highs.inspect})")
check(highs.sum - lows.sum == NMAIN, "watermarks account for exactly #{NMAIN} messages")

# seek_by: assign one partition, read one, seek back to 0, read it again. The two
# reads must be the same record -- that is what proves the seek reached the facade
# rather than being satisfied from a client-side buffer.
tpl = Rdkafka::Consumer::TopicPartitionList.new
tpl.add_topic_and_partitions_with_offsets(TOPIC_MAIN, 0 => 0)
probe.assign(tpl)
first = drain(probe, 1, 30).first
if first.nil?
  bad("assign(partition 0, offset 0) produced no message")
else
  ok("assign at an explicit offset returned index #{index_of(first.payload)} at offset #{first.offset}")
  skipped = drain(probe, 3, 30)
  probe.seek_by(TOPIC_MAIN, 0, first.offset)
  again = drain(probe, 1, 30).first
  check(again && again.offset == first.offset && again.payload.b == first.payload.b,
        "seek_by back to offset #{first.offset} re-delivered the identical record after reading #{skipped.size} more")
end

# The default-auto-commit group from section 3 should have left offsets behind.
committed_tpl = Rdkafka::Consumer::TopicPartitionList.new
committed_tpl.add_topic(TOPIC_MAIN, (0...PARTITIONS).to_a)
begin
  got = Rdkafka::Config.new(consumer_conf(GROUP_MAIN)).consumer
  c = got.committed(committed_tpl, 15_000)
  offs = c.to_h[TOPIC_MAIN].map(&:offset)
  info "committed offsets for #{GROUP_MAIN}: #{offs.inspect}"
  check(offs.compact.sum == NMAIN,
        "the auto-commit group committed #{offs.compact.sum} of #{NMAIN} (OffsetCommit + OffsetFetch round-trip)")
  got.close
rescue StandardError => e
  bad("committed() raised #{e.class}: #{e.message}")
end
probe.close

# ---------------------------------------------------------------- 7. resume
say "7. commit, stop, and resume in the SAME group from a NEW consumer instance"
a = Rdkafka::Config.new(consumer_conf(GROUP_RESUME, "enable.auto.commit" => "false")).consumer
a.subscribe(TOPIC_MAIN)
first_half = drain(a, RESUME_AT, 90)
check(first_half.size == RESUME_AT, "consumer A read #{first_half.size}/#{RESUME_AT} before committing")
begin
  a.commit(nil, false)
  ok("consumer A committed its positions synchronously")
rescue StandardError => e
  bad("consumer A commit raised #{e.class}: #{e.message}")
end
a.close
info "consumer A closed; a NEW instance now joins #{GROUP_RESUME}"

b = Rdkafka::Config.new(consumer_conf(GROUP_RESUME, "enable.auto.commit" => "false")).consumer
b.subscribe(TOPIC_MAIN)
second_half = drain(b, NMAIN - RESUME_AT, 90)
b.close

set_a = first_half.map { |m| index_of(m.payload) }.to_set
set_b = second_half.map { |m| index_of(m.payload) }.to_set
union = set_a | set_b
dups  = set_a & set_b
check(union.size == NMAIN, "A + B together saw all #{NMAIN} indices -- NO LOSS across the restart (saw #{union.size})")
check(second_half.size >= NMAIN - RESUME_AT,
      "consumer B resumed and read the remaining #{NMAIN - RESUME_AT} (got #{second_half.size})")
if dups.empty?
  ok("zero duplicates: B started exactly where A's commit left off")
else
  info "#{dups.size} indices were re-delivered to B (allowed: at-least-once redelivery of an uncommitted tail)"
  check(dups.size < NMAIN / 4, "redelivery is bounded (#{dups.size} < #{NMAIN / 4}), not a full rewind")
end

# ---------------------------------------------------------------- 8. SASL/TLS
# Optional lane, skipped entirely unless a SASL listener was named. The SASL
# PASSWORD is the Queen bearer token; the username is a free label the facade
# only logs. Verification is left ON unless KAFKA_SSL_INSECURE says otherwise --
# the rig's self-signed cert has no host.docker.internal SAN, so a containerised
# client has to be told to skip hostname verification or be given the CA.
say "8. SASL/PLAIN over TLS"
sasl_bootstrap = ENV["KAFKA_SASL_BOOTSTRAP"].to_s
if sasl_bootstrap.empty?
  info "skipped: KAFKA_SASL_BOOTSTRAP is unset"
else
  sasl_conf = {
    "security.protocol" => ENV.fetch("KAFKA_SASL_PROTOCOL", "sasl_ssl"),
    "sasl.mechanisms"   => "PLAIN",
    "sasl.username"     => ENV.fetch("KAFKA_SASL_USER", "rbrdk"),
    "sasl.password"     => ENV.fetch("KAFKA_SASL_TOKEN", "")
  }
  if ENV["KAFKA_SSL_CA"]
    sasl_conf["ssl.ca.location"] = ENV["KAFKA_SSL_CA"]
    sasl_conf["ssl.endpoint.identification.algorithm"] = "none" # cert has no SAN for the advertised name
  elsif ENV["KAFKA_SSL_INSECURE"] == "1"
    sasl_conf["enable.ssl.certificate.verification"] = "false"
  end
  topic_sasl = "rbrdk-sasl-#{RUN}"
  group_sasl = "rbrdk-gs-#{RUN}"
  dump_conf("sasl conf", sasl_conf.merge("sasl.password" => "<redacted>"))
  begin
    sp = Rdkafka::Config.new(producer_conf(sasl_conf.merge("bootstrap.servers" => sasl_bootstrap))).producer
    reports, errors = produce_range(sp, topic_sasl, 0...PARTITIONS, PARTITIONS)
    check(errors.zero? && reports.size == PARTITIONS,
          "#{reports.size}/#{PARTITIONS} messages produced over SASL_SSL (password = the Queen bearer token)")
    sp.close

    sc = Rdkafka::Config.new(consumer_conf(group_sasl, sasl_conf.merge("bootstrap.servers" => sasl_bootstrap))).consumer
    sc.subscribe(topic_sasl)
    sasl_msgs = drain(sc, PARTITIONS, 60)
    sc.close
    check(sasl_msgs.size == PARTITIONS, "#{sasl_msgs.size}/#{PARTITIONS} messages consumed by a GROUP over SASL_SSL")
    check(sasl_msgs.all? { |m| m.payload.b == payload_for(index_of(m.payload)).b },
          "SASL_SSL payloads are byte-exact")
  rescue StandardError => e
    bad("SASL_SSL lane raised #{e.class}: #{e.message}")
  end

  # A wrong password must be REFUSED, not silently accepted. Without this the
  # lane only proves TLS works, not that the credential is checked.
  if ENV["KAFKA_SASL_TOKEN"].to_s != ""
    begin
      wrong = sasl_conf.merge("bootstrap.servers" => sasl_bootstrap, "sasl.password" => "definitely-not-the-token")
      wp = Rdkafka::Config.new(producer_conf(wrong)).producer
      h = wp.produce(topic: topic_sasl, payload: "nope", partition: 0)
      begin
        wait_handle(h, 20_000)
        bad("a WRONG SASL password was accepted -- the credential is not being checked")
      rescue StandardError => e
        # NOTE ON THE SYMPTOM. librdkafka treats a SASL failure as RETRIABLE: it
        # logs the facade's real reason ("Queen refused this credential (HTTP
        # 401)") and starts a re-bootstrap loop, so what reaches Ruby is a
        # DELIVERY TIMEOUT rather than an authentication exception. The Java
        # client, by contrast, fails fast with SaslAuthenticationException. Grep
        # the protocol trace for the real reason -- it is not in this message.
        ok("a wrong SASL password is refused: #{e.class}: #{e.message.to_s[0, 160]}")
        refusal = File.foreach(TRACE).find { |l| l.include?("SASL authentication error") } rescue nil
        info("the reason, from librdkafka's log: #{refusal.strip.sub(/\A.*?SASL authentication error: /, '')[0, 200]}") if refusal
      end
      wp.close
    rescue StandardError => e
      ok("a wrong SASL password is refused at client construction/connect: #{e.class}: #{e.message.to_s[0, 160]}")
    end
  end
end

# ---------------------------------------------------------------- 9. negotiated versions
say "9. API versions this client actually NEGOTIATED (read from librdkafka debug=protocol)"
sleep 1  # the gem drains its log queue on a background thread
trace_io.flush
acc = Hash.new { |h, k| h[k] = [] }
codec_notes = []
begin
  File.foreach(TRACE) do |line|
    acc[$1] << Integer($2, 10) if line =~ /Sent (\w+?)Request \(v(\d+)/
    # librdkafka's own CODEC notice. It rides the MSG debug facility, which is
    # why base_conf sets `debug=protocol,msg` and not just protocol. It is also
    # rate limited to once per broker HANDLE, which is why section 5 gives each
    # codec its own producer. Matched narrowly on purpose: the banner line
    # librdkafka prints at startup lists ZSTD among builtin.features and would
    # otherwise be picked up instead.
    codec_notes << line.strip if line.include?("does not support compression type")
  end
rescue StandardError => e
  bad("could not read the protocol trace at #{TRACE}: #{e.message}")
end
# Freeze into a plain Hash: the default proc above would otherwise MATERIALISE a
# key on every miss, so `key?` after a read would answer yes for an API the client
# never sent.
sent = acc.transform_values { |v| v.uniq.sort }
if sent.empty?
  bad("librdkafka's debug=protocol stream produced no 'Sent xRequest' lines -- cannot report negotiated versions")
else
  sent.keys.sort.each { |k| info format("%-18s v%s", k, sent[k].join(",")) }
  check(sent.key?("ApiVersion"), "the connection began with ApiVersions, so every version below was negotiated and not assumed")
  check(sent["Fetch"]&.max&.<=(6), "Fetch negotiated down to v#{sent['Fetch']&.max} (facade caps at 6 on purpose: v7 is fetch sessions)")
  check(sent["Produce"]&.max&.<=(9), "Produce negotiated to v#{sent['Produce']&.max}")
  check(sent.key?("JoinGroup") && sent.key?("SyncGroup") && sent.key?("Heartbeat"),
        "the full group handshake ran: JoinGroup v#{sent['JoinGroup']&.join(',')}, SyncGroup v#{sent['SyncGroup']&.join(',')}, Heartbeat v#{sent['Heartbeat']&.join(',')}")
  check(!sent.key?("InitProducerId"), "the client never attempted InitProducerId (idempotence stayed off)")
end
# FAILS CLOSED, and it did not used to. This was `if codec_notes.empty? then
# info(...) else check(...)`: an EMPTY signal printed a note and the run passed,
# so the one outcome that must never pass quietly -- the detector going blind --
# was exactly the outcome that did. A compression regression hides in that
# branch: whether the facade started answering Fetch v10, or librdkafka reworded
# its notice, or the trace file stopped being written, the visible effect is the
# same missing line, and none of the three is something to shrug at.
#
# The downgrade is not optional here. librdkafka gates zstd PRODUCE on the
# broker advertising Fetch v10; queen-kafka caps Fetch at v6 on purpose
# (versions.rs -- v7 is fetch sessions), so EVERY run of section 5 must produce
# this notice. Its absence is a result, not a silence.
codec_notes.uniq.each { |l| info "librdkafka CODEC: #{l.sub(/\A.*?rdkafka: /, '')}" }
check(codec_notes.any? { |l| l.downcase.include?("zstd") },
      "librdkafka itself reported the zstd downgrade (#{codec_notes.size} codec notice(s)) -- the records in section 5 landed UNCOMPRESSED, which is the Fetch-v6 cap working as designed, not a defect")

# ---------------------------------------------------------------- done
producer.close
puts
if $fail.zero?
  puts "RESULT: PASS (rdkafka gem #{Rdkafka::VERSION} / librdkafka #{Rdkafka::LIBRDKAFKA_VERSION})"
  exit 0
else
  puts "RESULT: FAIL (#{$fail}) (rdkafka gem #{Rdkafka::VERSION} / librdkafka #{Rdkafka::LIBRDKAFKA_VERSION})"
  exit 1
end
