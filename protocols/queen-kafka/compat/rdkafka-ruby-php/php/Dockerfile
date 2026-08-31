# php-rdkafka (pecl) against queen-kafka.
#
# Unlike the Ruby gem, php-rdkafka does NOT vendor librdkafka: it links whatever
# the system provides, so the librdkafka version is a PACKAGING decision made
# here and not by the extension. Debian bookworm ships librdkafka 1.9.2 (2022),
# which predates a lot of what a modern client does; we build a current stable
# from source instead so this row of the matrix tests php-rdkafka rather than a
# four-year-old core. Both paths are documented in the README; switch by
# swapping the source build for `apt-get install -y librdkafka-dev`.
FROM php:8.3-cli-bookworm

ARG LIBRDKAFKA_VERSION=v2.6.1

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential pkg-config git ca-certificates curl \
      libssl-dev libsasl2-dev libzstd-dev liblz4-dev zlib1g-dev \
 && rm -rf /var/lib/apt/lists/*

# librdkafka from source. --enable-{ssl,sasl,zstd,lz4} are the features the
# suite actually exercises; configure would quietly drop any whose headers are
# missing, so they are named explicitly and the build fails loudly instead.
RUN git clone --depth 1 --branch "$LIBRDKAFKA_VERSION" https://github.com/confluentinc/librdkafka.git /tmp/librdkafka \
 && cd /tmp/librdkafka \
 && ./configure --prefix=/usr --enable-ssl --enable-sasl --enable-zstd --enable-lz4-ext \
 && make -j"$(nproc)" \
 && make install \
 && ldconfig \
 && rm -rf /tmp/librdkafka

# `pecl install rdkafka` asks one interactive question (libdir); the empty line
# takes the default, which is the /usr we just installed into.
RUN printf "\n" | pecl install rdkafka \
 && docker-php-ext-enable rdkafka \
 && php -m | grep -qx rdkafka

WORKDIR /app
# The entrypoint is the interpreter, not one script: run.sh picks compat.php or
# probe_compression.php and appends [bootstrap] [runId] after it.
COPY compat.php probe_compression.php ./
ENTRYPOINT ["php"]
CMD ["compat.php"]
