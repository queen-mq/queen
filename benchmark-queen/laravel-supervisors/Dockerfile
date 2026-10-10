# syntax=docker/dockerfile:1.7

# The stack under test. The defaults install the committed lock: Laravel 12
# on PHP 8.3, the campaigns' stack. BENCH_LARAVEL_VERSION resolves another
# Laravel (and BENCH_HORIZON_VERSION another Horizon) at build time instead,
# such as 11.55.1 on PHP 8.4.
ARG BENCH_PHP_VERSION=8.3
ARG BENCH_COMPOSER_VERSION=2.8.10

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


FROM php:${BENCH_PHP_VERSION}-cli-bookworm AS php-extensions

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
    && docker-php-ext-enable redis opcache \
    && rm -rf /tmp/pear /var/lib/apt/lists/*

# opcache is loaded in every lane but serves the command line only where the
# lane sets BENCH_OPCACHE_CLI=1 (PHP expands the variable when it reads the
# file; unset or 0 keeps it off).
RUN printf '%s\n' 'opcache.enable=1' 'opcache.enable_cli=${BENCH_OPCACHE_CLI}' \
    > /usr/local/etc/php/conf.d/zz-benchmark-opcache.ini


FROM composer:${BENCH_COMPOSER_VERSION} AS composer-bin


FROM php:${BENCH_PHP_VERSION}-cli-bookworm AS app

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
COPY --from=composer-bin /usr/bin/composer /usr/local/bin/composer
COPY --from=rust-supervisor /out/queen-supervisor /usr/local/bin/queen-supervisor

# The same binary as an offline release, packed as release-supervisor.yml packs one, for the
# lane that installs it as root for the user that runs it (queen-installed). The other
# platforms' archives are empty: the installer reads only this host's.
COPY supervisor/scripts/package_release.py supervisor/scripts/build_release_manifest.py /tmp/release-scripts/
COPY supervisor/dist/queen-supervisor.service.example LICENSE.md /tmp/release-staging/
RUN set -eu; \
    version="$(queen-supervisor --version)"; version="${version#queen-supervisor }"; \
    case "$(uname -m)" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; *) exit 1 ;; esac; \
    install -m 0755 /usr/local/bin/queen-supervisor /tmp/release-staging/queen-supervisor; \
    release=/opt/queen-supervisor-release; \
    python3 /tmp/release-scripts/package_release.py --staging /tmp/release-staging --output "$release" \
        --version "$version" --os linux --arch "$arch" --source-date-epoch 0; \
    for platform in linux-amd64 linux-arm64 darwin-amd64 darwin-arm64; do \
        [ -e "$release/queen-supervisor-$version-$platform.tar.gz" ] || : > "$release/queen-supervisor-$version-$platform.tar.gz"; \
    done; \
    python3 /tmp/release-scripts/build_release_manifest.py --version "$version" --repository queen-mq/queen \
        --source-commit 0000000000000000000000000000000000000000 --dist "$release" \
        --output "$release/queen-supervisor-manifest.json"; \
    ln -s "queen-supervisor-$version-linux-$arch.tar.gz" "$release/host.tar.gz"; \
    rm -rf /tmp/release-scripts /tmp/release-staging

# Preserve this layout: the fixture's Composer path repository deliberately
# resolves ../../../clients/client-php to the client from this checkout.
WORKDIR /workspace/benchmark-queen/laravel-supervisors/app
COPY --chown=${APP_UID}:${APP_GID} clients/client-php /workspace/clients/client-php
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

# Another Laravel than the lock's is resolved here, against this image's PHP.
# Composer 2.9 and later refuse a version with a security advisory; the IDs in
# BENCH_COMPOSER_ADVISORY_IGNORE_IDS (space-separated; Composer 2.9 or later
# reads the setting) are let through, for a test image only. The resolved
# lock stays in the image, and the build log lists the advisories.
ARG BENCH_LARAVEL_VERSION=
ARG BENCH_HORIZON_VERSION=5.48.3
ARG BENCH_COMPOSER_ADVISORY_IGNORE_IDS=
# A released PHP client from Packagist, such as 2.4.3, instead of the
# checkout's: the previous release of a rolling upgrade (queen-previous).
ARG BENCH_PHP_CLIENT_VERSION=

RUN --mount=type=cache,target=/home/benchmark/.cache/composer,uid=1000,gid=1000 \
    if [ -n "${BENCH_PHP_CLIENT_VERSION}" ]; then \
        composer config --unset repositories.0 \
        && composer require --no-update --no-interaction "queen-mq/php-client:${BENCH_PHP_CLIENT_VERSION}" \
        && composer update queen-mq/php-client \
            --with-dependencies \
            --no-dev \
            --no-interaction \
            --no-progress \
            --prefer-dist \
            --classmap-authoritative; \
    elif [ -z "${BENCH_LARAVEL_VERSION}" ]; then \
        composer install \
            --no-dev \
            --no-interaction \
            --no-progress \
            --prefer-dist \
            --classmap-authoritative; \
    else \
        composer config platform.php "$(php -r 'echo PHP_VERSION;')" \
        && if [ -n "${BENCH_COMPOSER_ADVISORY_IGNORE_IDS}" ]; then \
            composer config --json policy.advisories.ignore-id \
                "$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1].split()))' "${BENCH_COMPOSER_ADVISORY_IGNORE_IDS}")"; \
        fi \
        && composer require --no-update --no-interaction \
            "laravel/framework:${BENCH_LARAVEL_VERSION}" "laravel/horizon:${BENCH_HORIZON_VERSION}" \
        && composer update \
            --no-dev \
            --no-interaction \
            --no-progress \
            --prefer-dist \
            --classmap-authoritative \
        && { composer audit --locked --no-interaction || true; }; \
    fi \
    && php artisan bench:config >/dev/null \
    && queen-supervisor --version

# As docs/guides/laravel/kubernetes builds an image: root installs the release for the user
# that runs it (--owner), in the build. Only the queen-installed lane launches it; a released
# client pins its own supervisor, not the checkout's, so an image of one has none.
USER 0:0
RUN if [ -z "${BENCH_PHP_CLIENT_VERSION}" ]; then \
        manifest=/opt/queen-supervisor-release/queen-supervisor-manifest.json \
        && QUEEN_SUPERVISOR_INSTALL_PATH=/opt/queen-supervisor-bin php artisan --no-ansi queen:supervisor-install \
            --manifest="$manifest" --archive="$(readlink -f /opt/queen-supervisor-release/host.tar.gz)" \
            --manifest-sha256="$(sha256sum "$manifest" | cut -d' ' -f1)" --owner="${APP_UID}"; \
    fi
USER ${APP_UID}:${APP_GID}

CMD ["php", "artisan", "list", "--no-ansi"]
