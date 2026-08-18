#!/usr/bin/env bash
# The HTTP suite, in the order the JS runner established: the broker-free unit
# suite first, then the integration one.
#
# The unit suite runs FIRST on purpose. It needs no broker and no database, so if
# a body shape is wrong it says so in a second, with the exact bytes, instead of
# being reported thirty seconds later as an unexplained 400 from a stored
# procedure. And if it is red, the integration run that follows is asserting
# against a client that is already known to be wrong.
set -u

fail=0

/suite/http-wire-unit.sh || fail=1

# The integration half needs the broker up; the unit half does not, which is why
# the wait is here and not at the top.
/usr/local/bin/wait-for-broker || exit 97

/suite/http-wire-check.sh || fail=1

exit "$fail"
