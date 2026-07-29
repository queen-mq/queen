package cmd

import (
	"errors"
	"fmt"
	"net/http"

	clierr "github.com/smartpricing/queen/clients/client-cli/internal/errors"
	queen "github.com/smartpricing/queen/clients/client-go"
)

// codeRouteBlocked is queen-proxy's machine-readable code for an
// operator-only broker surface. The proxy fails closed on those routes and
// answers 404 (not 403) so a tenant learns nothing about the broker's shape -
// which means the SDK error reaching the user is indistinguishable from a
// typo'd URL unless the command says what it was reaching for.
const codeRouteBlocked = "route_blocked"

// blockedErr classifies an SDK error from a command whose broker route the
// proxy may refuse. `surface` names that route and `alt` says what to run
// instead (empty when nothing replaces it). Any other error keeps the
// standard exit code 2; a blocked route is exit 1, since no retry helps and
// the fix is to run a different command.
func blockedErr(err error, surface, alt string) error {
	var he *queen.HTTPError
	if errors.As(err, &he) && he.StatusCode == http.StatusNotFound && he.Code == codeRouteBlocked {
		msg := fmt.Sprintf("%s is an operator-only broker surface; queen-proxy does not expose it to tenant credentials", surface)
		if alt != "" {
			msg += "; " + alt
		}
		return clierr.Userf("%s", msg)
	}
	return clierr.Server(err)
}
