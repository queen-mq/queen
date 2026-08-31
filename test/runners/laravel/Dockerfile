# PHP/Laravel client integration runner. Build context = repo root.
#   docker build -f test/runners/laravel/Dockerfile -t queen-test-runner-laravel .
# Exercise the package's declared minimum, including its Laravel queue driver
# and supervisor configuration contract. composer.json pins dependency
# resolution to PHP 8.3 even when the build runs from a newer maintainer
# workstation. This is a library package, so composer.lock is intentionally
# not versioned.
FROM php:8.3-cli-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates git unzip libonig-dev libxml2-dev \
 && docker-php-ext-install -j"$(nproc)" mbstring pcntl dom xml xmlwriter \
 && rm -rf /var/lib/apt/lists/*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /src

# Manifests first for layer caching. mbstring is required by Laravel,
# dom/xmlwriter by PHPUnit, and pcntl exercises the Unix supervisor path.
COPY clients/client-laravel/composer.json ./
RUN composer install --no-interaction --no-progress --no-scripts

COPY clients/client-laravel/ ./

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/laravel/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
