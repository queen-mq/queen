package provider

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/knownvalue"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/statecheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
	"github.com/hashicorp/terraform-plugin-testing/tfjsonpath"
	"github.com/hashicorp/terraform-plugin-testing/tfversion"
)

// The acceptance tests run real plans and applies against a live broker:
//
//	TF_ACC=1 QUEEN_ENDPOINT=http://localhost:6632 go test ./internal/provider/
//
// The S3 sink's tests also need the proxy's control plane
// (QUEEN_CP_ENDPOINT, QUEEN_CP_TOKEN), a cluster (QUEEN_TEST_CLUSTER, default
// "acme") and a broker started with QUEEN_S3_EMBEDDED=true and an encryption
// key. They are skipped without them.

var factories = map[string]func() (tfprotov6.ProviderServer, error){
	"queen": providerserver.NewProtocol6WithError(New("test")()),
}

func preCheck(t *testing.T) {
	t.Helper()
	if os.Getenv("QUEEN_ENDPOINT") == "" {
		t.Fatal("QUEEN_ENDPOINT must name a broker for the acceptance tests")
	}
}

func testClient() *client {
	return newClient(os.Getenv("QUEEN_ENDPOINT"), os.Getenv("QUEEN_TOKEN"), os.Getenv("QUEEN_CP_ENDPOINT"), os.Getenv("QUEEN_CP_TOKEN"))
}

// push sends one message: the way a queue comes to exist without this provider.
func push(t *testing.T, queue string) {
	t.Helper()
	body := fmt.Sprintf(`{"items":[{"queue":%q,"partition":"p","payload":{"n":1}}]}`, queue)
	res, err := http.Post(os.Getenv("QUEEN_ENDPOINT")+"/api/v1/push", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode/100 != 2 {
		t.Fatalf("push answered %d", res.StatusCode)
	}
}

func queueExists(name string) (bool, error) {
	_, err := testClient().getQueue(context.Background(), name)
	if notFound(err) {
		return false, nil
	}
	return err == nil, err
}

func value(resourceName, attr string, want knownvalue.Check) statecheck.StateCheck {
	return statecheck.ExpectKnownValue(resourceName, tfjsonpath.New(attr), want)
}

// A queue is created with the options named, the rest are the broker's, an
// update changes only what it names, and the resource imports by name.
func TestAccQueue_createUpdateImport(t *testing.T) {
	const name = "tfacc.orders"
	const r = "queen_queue.q"
	t.Cleanup(func() { _ = testClient().deleteQueue(context.Background(), name) })
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: factories,
		CheckDestroy: func(*terraform.State) error {
			if ok, err := queueExists(name); err != nil || ok {
				return fmt.Errorf("delete_on_destroy was true and the queue is still there (%v)", err)
			}
			return nil
		},
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`resource "queen_queue" "q" {
  name              = %q
  lease_time        = 30
  retry_limit       = 5
  delete_on_destroy = true
}`, name),
				ConfigStateChecks: []statecheck.StateCheck{
					value(r, "id", knownvalue.StringExact(name)),
					value(r, "lease_time", knownvalue.Int64Exact(30)),
					value(r, "retry_limit", knownvalue.Int64Exact(5)),
					// The broker's own defaults, read back.
					value(r, "dedup_window_seconds", knownvalue.Int64Exact(3600)),
					value(r, "dead_letter_queue", knownvalue.Bool(true)),
					value(r, "retention_enabled", knownvalue.Bool(false)),
					value(r, "encryption_enabled", knownvalue.Bool(false)),
				},
			},
			{
				Config: fmt.Sprintf(`resource "queen_queue" "q" {
  name              = %q
  lease_time        = 30
  retention_enabled = true
  retention_seconds = 86400
  namespace         = "billing"
  task              = "invoices"
  delete_on_destroy = true
}`, name),
				ConfigStateChecks: []statecheck.StateCheck{
					value(r, "retention_enabled", knownvalue.Bool(true)),
					value(r, "retention_seconds", knownvalue.Int64Exact(86400)),
					value(r, "namespace", knownvalue.StringExact("billing")),
					value(r, "task", knownvalue.StringExact("invoices")),
					// Left out of this configuration: kept as it was.
					value(r, "retry_limit", knownvalue.Int64Exact(5)),
				},
			},
			{
				ResourceName:            r,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"delete_on_destroy"},
			},
		},
	})
}

// A queue that a push created is taken over without touching what the
// configuration does not name: its labels and its other options stay.
func TestAccQueue_takesOverAnExistingQueue(t *testing.T) {
	const name = "tfacc2.adopted"
	const r = "queen_queue.q"
	t.Cleanup(func() { _ = testClient().deleteQueue(context.Background(), name) })
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: factories,
		// delete_on_destroy is false: the queue and its message must survive.
		CheckDestroy: func(*terraform.State) error {
			if ok, err := queueExists(name); err != nil || !ok {
				return fmt.Errorf("the queue was deleted although delete_on_destroy is false (%v)", err)
			}
			return nil
		},
		Steps: []resource.TestStep{
			{
				PreConfig: func() { push(t, name) },
				Config: fmt.Sprintf(`resource "queen_queue" "q" {
  name        = %q
  retry_limit = 7
}`, name),
				ConfigStateChecks: []statecheck.StateCheck{
					value(r, "retry_limit", knownvalue.Int64Exact(7)),
					// A queue made by a push takes its labels from its name.
					value(r, "namespace", knownvalue.StringExact("tfacc2")),
					value(r, "task", knownvalue.StringExact("adopted")),
					value(r, "lease_time", knownvalue.Int64Exact(60)),
					value(r, "delete_on_destroy", knownvalue.Bool(false)),
				},
			},
			{
				// Someone changes the option by hand: the next plan puts it back.
				PreConfig: func() {
					seven, three := int64(7), int64(3)
					_ = seven
					if err := testClient().configureQueue(context.Background(), name, nil, nil, queueOptions{RetryLimit: &three}); err != nil {
						t.Fatal(err)
					}
				},
				Config: fmt.Sprintf(`resource "queen_queue" "q" {
  name        = %q
  retry_limit = 7
}`, name),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction(r, plancheck.ResourceActionUpdate)},
				},
				ConfigStateChecks: []statecheck.StateCheck{value(r, "retry_limit", knownvalue.Int64Exact(7))},
			},
		},
	})
}

func TestAccQueue_refusesANegativeValue(t *testing.T) {
	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { preCheck(t) },
		ProtoV6ProviderFactories: factories,
		Steps: []resource.TestStep{{
			Config: `resource "queen_queue" "q" {
  name       = "tfacc.never"
  lease_time = -1
}`,
			ExpectError: regexp.MustCompile(`must not be negative`),
		}},
	})
}

func s3PreCheck(t *testing.T) string {
	t.Helper()
	preCheck(t)
	if os.Getenv("QUEEN_CP_ENDPOINT") == "" || os.Getenv("QUEEN_CP_TOKEN") == "" {
		t.Skip("QUEEN_CP_ENDPOINT and QUEEN_CP_TOKEN are not set: the S3 sink is set through the control plane")
	}
	cluster := os.Getenv("QUEEN_TEST_CLUSTER")
	if cluster == "" {
		cluster = "acme"
	}
	return cluster
}

func s3Config(cluster, bucket string, version int, extra string) string {
	return fmt.Sprintf(`resource "queen_s3_sink" "lake" {
  cluster            = %q
  enabled            = false
  queues             = "*"
  endpoint           = "https://s3.eu-central-1.amazonaws.com"
  region             = "eu-central-1"
  bucket             = %q
  access_key         = "AKIAEXAMPLE"
  secret_key         = "not-a-real-secret"
  secret_key_version = %d
%s}`, cluster, bucket, version, extra)
}

// The sink is stored as written, its secret is sent and never kept in the
// state, a change sends the settings without the secret, a new version sends
// the secret again, and the resource imports by the cluster's slug.
func TestAccS3Sink_createUpdateImport(t *testing.T) {
	cluster := s3PreCheck(t)
	const r = "queen_s3_sink.lake"
	t.Cleanup(func() { _ = testClient().deleteS3Sink(context.Background(), cluster) })
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: factories,
		// Write-only arguments.
		TerraformVersionChecks: []tfversion.TerraformVersionCheck{tfversion.SkipBelow(tfversion.Version1_11_0)},
		CheckDestroy: func(*terraform.State) error {
			_, err := testClient().getS3Sink(context.Background(), cluster)
			if !notFound(err) {
				return fmt.Errorf("the sink is still there after the destroy (%v)", err)
			}
			return nil
		},
		Steps: []resource.TestStep{
			{
				Config: s3Config(cluster, "acme-lake", 1, "  format             = \"parquet\"\n"),
				ConfigStateChecks: []statecheck.StateCheck{
					value(r, "id", knownvalue.StringExact(cluster)),
					value(r, "bucket", knownvalue.StringExact("acme-lake")),
					value(r, "format", knownvalue.StringExact("parquet")),
					value(r, "enabled", knownvalue.Bool(false)),
					value(r, "secret_key_set", knownvalue.Bool(true)),
					// The secret reached the broker and nothing of it is in the state.
					value(r, "secret_key", knownvalue.Null()),
					// A setting left out is not invented.
					value(r, "prefix", knownvalue.Null()),
				},
			},
			{
				Config: s3Config(cluster, "acme-lake-2", 1, "  format             = \"parquet\"\n  target_mb          = 64\n  path_style         = true\n"),
				ConfigStateChecks: []statecheck.StateCheck{
					value(r, "bucket", knownvalue.StringExact("acme-lake-2")),
					value(r, "target_mb", knownvalue.Int64Exact(64)),
					value(r, "path_style", knownvalue.Bool(true)),
					value(r, "secret_key_set", knownvalue.Bool(true)),
				},
			},
			{
				// The key was rotated: the version moves and the secret goes again.
				Config: s3Config(cluster, "acme-lake-2", 2, "  format             = \"parquet\"\n  target_mb          = 64\n  path_style         = true\n"),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PreApply: []plancheck.PlanCheck{plancheck.ExpectResourceAction(r, plancheck.ResourceActionUpdate)},
				},
				ConfigStateChecks: []statecheck.StateCheck{value(r, "secret_key_version", knownvalue.Int64Exact(2))},
			},
			{
				ResourceName:            r,
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"secret_key", "secret_key_version"},
			},
		},
	})
}

func TestAccS3Sink_refusesWhatTheBrokerRefuses(t *testing.T) {
	cluster := s3PreCheck(t)
	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: factories,
		TerraformVersionChecks:   []tfversion.TerraformVersionCheck{tfversion.SkipBelow(tfversion.Version1_11_0)},
		Steps: []resource.TestStep{
			{
				// A new sink with no secret: the broker has none to keep.
				Config: fmt.Sprintf(`resource "queen_s3_sink" "lake" {
  cluster    = %q
  queues     = "*"
  endpoint   = "https://s3.eu-central-1.amazonaws.com"
  region     = "eu-central-1"
  bucket     = "acme-lake"
  access_key = "AKIAEXAMPLE"
}`, cluster),
				ExpectError: regexp.MustCompile(`A new sink needs secret_key`),
			},
			{
				// The broker's own rule, passed on as it says it.
				Config:      s3Config(cluster, "acme-lake", 1, "  format             = \"csv\"\n"),
				ExpectError: regexp.MustCompile(`(?s)could not be set.*format`),
			},
			{
				Config:      s3Config("no-such-cluster", "acme-lake", 1, ""),
				ExpectError: regexp.MustCompile(`no such cluster`),
			},
		},
	})
}
