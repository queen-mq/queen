#!/usr/bin/env bash
# PHP client suite: the unit tests (no broker needed, and they are the ones that
# pin the exact JSON wire shape) followed by the integration tree.
#
# The unit run goes first on purpose: if the wire shape is wrong, its failure
# names the field, while the integration failure would only say the broker
# answered 400.
set -eu

export QUEEN_HTTP_URL="$QUEEN_HTTP_URL"

/usr/local/bin/wait-for-broker

cd /src
vendor/bin/phpunit --testsuite Unit
exec vendor/bin/phpunit --testsuite Integration --testdox
