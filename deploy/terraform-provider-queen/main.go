// terraform-provider-queen declares what lives inside a Queen broker: the
// configuration of its queues and a cluster's S3 sink.
package main

import (
	"context"
	"flag"
	"log"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"

	"github.com/queen-mq/terraform-provider-queen/internal/provider"
)

// Set by the release build.
var version = "dev"

func main() {
	var debug bool
	flag.BoolVar(&debug, "debug", false, "run the provider for a debugger to attach to")
	flag.Parse()

	err := providerserver.Serve(context.Background(), provider.New(version), providerserver.ServeOpts{
		Address: "registry.terraform.io/queen-mq/queen",
		Debug:   debug,
	})
	if err != nil {
		log.Fatal(err)
	}
}
