# PHP/Laravel client integration runner. Build context = repo root.
#   docker build -f test/runners/laravel/Dockerfile -t queen-test-runner-laravel .
# 8.4 and not 8.3, which is the library's declared minimum: the committed
# composer.lock pins dev dependencies (orchestra/testbench pulls symfony 8) that
# require php >= 8.4, so 8.3 cannot install the lock at all. The library itself
# still declares ^8.3 and uses nothing newer; closing that gap means relaxing
# the dev dependencies, which is a composer.json change and not this suite's.
FROM php:8.4-cli-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates git unzip \
 && rm -rf /var/lib/apt/lists/*

COPY --from=composer:2 /usr/bin/composer /usr/bin/composer

WORKDIR /src

# Manifests first for layer caching. The client needs no PHP extension beyond
# the base image's (guzzle rides on the bundled curl/json), so there is no
# pecl/docker-php-ext step and nothing to compile.
COPY clients/client-laravel/composer.json clients/client-laravel/composer.lock ./
RUN composer install --no-interaction --no-progress --no-scripts

COPY clients/client-laravel/ ./

COPY test/runners/common/wait-for-broker.sh /usr/local/bin/wait-for-broker
COPY test/runners/laravel/entrypoint.sh /entrypoint.sh
RUN chmod +x /usr/local/bin/wait-for-broker /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
