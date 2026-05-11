package cmd

import "github.com/smartpricing/queen/clients/client-cli/internal/sdk"

// sdkClient is a package-local alias used in command-handler signatures so
// they don't import the long path each time.
type sdkClient = sdk.Client
