# The `rdkafka` gem (karafka's core) against queen-kafka.
#
# The gem VENDORS librdkafka: `bundle install` downloads the pinned librdkafka
# tarball and compiles it here, which is why this image takes minutes to build
# and seconds to run. The dev headers below are what that vendored build probes
# for; without libzstd-dev/liblz4-dev/libsasl2-dev/libssl-dev the gem still
# installs, it just silently produces a librdkafka with those features missing,
# and you find out at runtime as "Unsupported compression type" or a SASL
# mechanism that does not exist. Install them BEFORE bundle install.
FROM ruby:3.3-bookworm

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential pkg-config \
      libssl-dev libsasl2-dev libzstd-dev liblz4-dev zlib1g-dev \
      ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY Gemfile Gemfile.lock* ./
RUN bundle config set --local without development:test \
 && bundle install \
 && bundle list

# The entrypoint is the interpreter, not one script: run.sh picks compat.rb or
# probe_compression.rb and appends [bootstrap] [runId] after it.
COPY compat.rb probe_compression.rb ./
ENTRYPOINT ["bundle", "exec", "ruby"]
CMD ["compat.rb"]
