# syntax=docker/dockerfile:1.7

# Build the supervisor from the exact source tree under test. Keeping this in
# the benchmark image avoids accidentally comparing the PHP engine with a
# previously published Rust binary.
FROM rust:1.88-bookworm AS rust-supervisor

WORKDIR /build
COPY supervisor/Cargo.toml supervisor/Cargo.lock ./
COPY supervisor/src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked \
    && install -Dm0755 target/release/queen-supervisor /out/queen-supervisor


FROM php:8.3-cli-bookworm AS php-extensions

ARG PHP_REDIS_VERSION=6.1.0

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        $PHPIZE_DEPS \
        libonig-dev \
        libxml2-dev \
    && docker-php-ext-install -j"$(nproc)" \
        bcmath \
        dom \
        mbstring \
        pcntl \
        posix \
    && pecl install "redis-${PHP_REDIS_VERSION}" \
    && docker-php-ext-enable redis \
    && rm -rf /tmp/pear /var/lib/apt/lists/*


FROM php:8.3-cli-bookworm AS app

ARG APP_UID=1000
ARG APP_GID=1000

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
        libonig5 \
        libxml2 \
        procps \
        python3 \
        unzip \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${APP_GID}" benchmark \
    && useradd --uid "${APP_UID}" --gid "${APP_GID}" \
        --create-home --shell /bin/bash benchmark

COPY --from=php-extensions /usr/local/lib/php/extensions/ /usr/local/lib/php/extensions/
COPY --from=php-extensions /usr/local/etc/php/conf.d/ /usr/local/etc/php/conf.d/
COPY --from=composer:2.8.10 /usr/bin/composer /usr/local/bin/composer
COPY --from=rust-supervisor /out/queen-supervisor /usr/local/bin/queen-supervisor

# Preserve this layout: the fixture's Composer path repository deliberately
# resolves ../../../clients/client-laravel to the client from this checkout.
WORKDIR /workspace/benchmark-queen/laravel-supervisors/app
COPY --chown=${APP_UID}:${APP_GID} clients/client-laravel /workspace/clients/client-laravel
COPY --chown=${APP_UID}:${APP_GID} benchmark-queen/laravel-supervisors/app ./

RUN install -d -o "${APP_UID}" -g "${APP_GID}" -m 0770 \
        bootstrap/cache \
        storage/framework/cache \
        storage/framework/sessions \
        storage/framework/views \
        storage/logs \
        /results \
    && install -d -o "${APP_UID}" -g "${APP_GID}" -m 0700 /run/queen-supervisor \
    && chown "${APP_UID}:${APP_GID}" .

ENV HOME=/home/benchmark \
    COMPOSER_HOME=/home/benchmark/.cache/composer

USER ${APP_UID}:${APP_GID}

RUN --mount=type=cache,target=/home/benchmark/.cache/composer,uid=1000,gid=1000 \
    composer install \
        --no-dev \
        --no-interaction \
        --no-progress \
        --prefer-dist \
        --classmap-authoritative \
    && php artisan bench:config >/dev/null \
    && queen-supervisor --version

CMD ["php", "artisan", "list", "--no-ansi"]
