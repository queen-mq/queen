package main

// tenantfile.go — REAL tenant provisioning for the proxy campaign (-mode provision)
// plus the credential file every cloud run reads.
//
// A simulated tenant in -mode cloud is a real queen_proxy tenant: its own
// tenant row, its own cluster (= the routing subdomain label), its own API key
// and its own broker tenant UUID. Provisioning goes through
// queen_proxy.bootstrap_tenant on the cell's pxdb — the same function the
// operator runbook uses — and the PLAINTEXT api key it returns is shown exactly
// once, so it is cached to a file. A re-run reads the cache and provisions only
// the indices that are missing: keys are unrecoverable, so re-provisioning an
// existing cluster would have to mint a new key, and silently rotating
// credentials under a running campaign is exactly the kind of surprise a bench
// does not need.
//
//	goload -mode provision -tenants 50 -file /root/campaign/tenants.json
//
// The DB call is one psql invocation for the whole batch (a per-tenant
// docker-exec round trip costs ~200ms, which is 3+ minutes for 1000 tenants).

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// TenantCred is one simulated tenant's full identity.
type TenantCred struct {
	Idx          int    `json:"idx"`
	TenantSlug   string `json:"tenantSlug"`
	ClusterSlug  string `json:"clusterSlug"`  // Host header first label -> routing
	TenantID     string `json:"tenantId"`     // queen_proxy.tenants.id
	ClusterID    string `json:"clusterId"`    // queen_proxy.clusters.id
	BrokerTenant string `json:"brokerTenant"` // clusters.broker_tenant_uuid -> x-queen-tenant
	APIKey       string `json:"apiKey"`       // plaintext, unrecoverable once lost
	Plan         string `json:"plan"`
}

// TenantsFile is the on-disk cache.
type TenantsFile struct {
	Cell        string       `json:"cell"`
	Plan        string       `json:"plan"`
	Prefix      string       `json:"prefix"`
	GeneratedAt string       `json:"generatedAt"`
	Tenants     []TenantCred `json:"tenants"`
}

const defaultPsqlCmd = "docker exec -i cell-pxdb psql -qtA -v ON_ERROR_STOP=1 -U postgres -d queen_proxy"

func loadTenantsFile(path string) (*TenantsFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var tf TenantsFile
	if err := json.Unmarshal(b, &tf); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return &tf, nil
}

func saveTenantsFile(path string, tf *TenantsFile) error {
	if d := filepath.Dir(path); d != "" && d != "." {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
	}
	b, err := json.MarshalIndent(tf, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(b, '\n'), 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func runProvisionMode(args []string) {
	fs := flag.NewFlagSet("goload-provision", flag.ExitOnError)
	n := fs.Int("tenants", 2, "number of tenants that must exist (indices 0..N-1)")
	prefix := fs.String("prefix", "camp", "slug prefix: tenant slug = cluster slug = <prefix>-NNNN (the cluster slug IS the routing Host label, so keep it a valid DNS label)")
	plan := fs.String("plan", "bench", "plan code for every provisioned cluster")
	cell := fs.String("cell", "bench", "cell slug the clusters are placed on")
	file := fs.String("file", "tenants.json", "credential cache file (created/extended, never rewritten for existing indices)")
	psqlCmd := fs.String("psql-cmd", defaultPsqlCmd, "shell command that pipes stdin SQL into the cell's pxdb")
	keyName := fs.String("key-name", "", "API key name (default: bench-<unix>, unique per provisioning run so a cluster that already exists still yields a usable key)")
	emailDom := fs.String("email-domain", "bench.invalid", "admin email domain")
	dry := fs.Bool("dry-run", false, "print the SQL and exit")
	_ = fs.String("mode", "provision", "run mode")
	_ = fs.Parse(args)

	if *n <= 0 || *n > 10000 {
		fmt.Println("goload -mode provision: -tenants must be in 1..10000")
		os.Exit(2)
	}

	tf, err := loadTenantsFile(*file)
	if err != nil {
		if !os.IsNotExist(err) {
			fmt.Printf("[provision] cannot read %s: %v\n", *file, err)
			os.Exit(1)
		}
		tf = &TenantsFile{Cell: *cell, Plan: *plan, Prefix: *prefix}
	}
	have := map[int]bool{}
	for _, t := range tf.Tenants {
		if t.APIKey != "" {
			have[t.Idx] = true
		}
	}
	var need []int
	for i := 0; i < *n; i++ {
		if !have[i] {
			need = append(need, i)
		}
	}
	if len(need) == 0 {
		fmt.Printf("[provision] %s already holds %d/%d tenants with keys — nothing to do\n", *file, *n, *n)
		return
	}

	kn := *keyName
	if kn == "" {
		kn = fmt.Sprintf("bench-%d", time.Now().Unix())
	}

	sql := buildProvisionSQL(need, *prefix, *plan, *cell, *emailDom, kn)
	if *dry {
		fmt.Println(sql)
		fmt.Println(buildBrokerTenantSQL(need, *prefix))
		return
	}

	fmt.Printf("[provision] %d tenant(s) to create (%v...) plan=%s cell=%s key=%s\n",
		len(need), need[:minInt(3, len(need))], *plan, *cell, kn)
	t0 := time.Now()
	out, err := runPsql(*psqlCmd, sql)
	if err != nil {
		fmt.Printf("[provision] FAILED: %v\n%s\n", err, out)
		os.Exit(1)
	}

	fresh := make([]TenantCred, 0, len(need))
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		f := strings.Split(line, "\t")
		if len(f) != 5 {
			fmt.Printf("[provision] unexpected psql row (%d fields): %q\n", len(f), line)
			os.Exit(1)
		}
		idx, cerr := strconv.Atoi(f[0])
		if cerr != nil {
			fmt.Printf("[provision] bad index in row %q\n", line)
			os.Exit(1)
		}
		if f[4] == "" {
			fmt.Printf("[provision] tenant %s: bootstrap_tenant returned a NULL api_key (a key named %q already exists on that cluster and the plaintext is unrecoverable). Re-run with -key-name <fresh>.\n", f[1], kn)
			os.Exit(1)
		}
		fresh = append(fresh, TenantCred{
			Idx: idx, TenantSlug: f[1], ClusterSlug: f[1],
			TenantID: f[2], ClusterID: f[3], APIKey: f[4], Plan: *plan,
		})
	}
	if len(fresh) != len(need) {
		fmt.Printf("[provision] FAILED: asked for %d tenants, psql returned %d rows\n", len(need), len(fresh))
		os.Exit(1)
	}

	// broker_tenant_uuid must be read in a SEPARATE statement. A single statement
	// that JOINed queen_proxy.clusters would see the pre-statement snapshot, in
	// which the clusters bootstrap_tenant just inserted DO NOT EXIST — the join
	// silently drops every new tenant and returns zero rows while the bootstrap
	// itself commits. (That is not hypothetical: it is what the first version of
	// this code did.)
	out2, err := runPsql(*psqlCmd, buildBrokerTenantSQL(need, *prefix))
	if err != nil {
		fmt.Printf("[provision] tenants created but broker_tenant_uuid lookup FAILED: %v\n%s\n", err, out2)
		os.Exit(1)
	}
	bt := map[string]string{}
	for _, line := range strings.Split(strings.TrimSpace(out2), "\n") {
		if f := strings.Split(strings.TrimSpace(line), "\t"); len(f) == 2 {
			bt[f[0]] = f[1]
		}
	}
	added := 0
	for i := range fresh {
		u, ok := bt[fresh[i].ClusterSlug]
		if !ok || u == "" {
			fmt.Printf("[provision] FAILED: no broker_tenant_uuid for cluster %s\n", fresh[i].ClusterSlug)
			os.Exit(1)
		}
		fresh[i].BrokerTenant = u
		tf.Tenants = append(tf.Tenants, fresh[i])
		added++
	}

	sortTenants(tf.Tenants)
	tf.Cell, tf.Plan, tf.Prefix = *cell, *plan, *prefix
	tf.GeneratedAt = time.Now().UTC().Format(time.RFC3339)
	if err := saveTenantsFile(*file, tf); err != nil {
		fmt.Printf("[provision] cannot write %s: %v\n", *file, err)
		os.Exit(1)
	}
	fmt.Printf("[provision] +%d tenants in %.1fs -> %s now holds %d (first=%s last=%s)\n",
		added, time.Since(t0).Seconds(), *file, len(tf.Tenants),
		tf.Tenants[0].ClusterSlug, tf.Tenants[len(tf.Tenants)-1].ClusterSlug)
}

// buildProvisionSQL creates every missing tenant in ONE statement, so the whole
// batch is one transaction: a failure anywhere unwinds all of it (bootstrap_tenant
// is plpgsql and runs in the caller's transaction) rather than leaving a
// half-provisioned fleet behind. It returns ONLY what bootstrap_tenant itself
// hands back — anything read out of a table the function just wrote to would be
// invisible to this statement's snapshot (see the note at the call site).
func buildProvisionSQL(idx []int, prefix, plan, cell, emailDom, keyName string) string {
	var vals []string
	for _, i := range idx {
		vals = append(vals, fmt.Sprintf("(%d, '%s-%04d')", i, sqlLit(prefix), i))
	}
	return fmt.Sprintf(`\set ON_ERROR_STOP on
WITH c AS (SELECT id FROM queen_proxy.cells WHERE slug = '%s'),
     s(i, slug) AS (VALUES %s),
     b AS (
       SELECT s.i, s.slug,
              queen_proxy.bootstrap_tenant(
                s.slug, s.slug, s.slug, '%s', c.id,
                s.slug || '@%s', NULL, '%s') AS r
       FROM s CROSS JOIN c)
SELECT b.i::text || E'\t' || b.slug || E'\t' || (b.r->>'tenant_id') || E'\t'
       || (b.r->>'cluster_id') || E'\t' || coalesce(b.r->>'api_key', '')
FROM b ORDER BY b.i;
`, sqlLit(cell), strings.Join(vals, ", "), sqlLit(plan), sqlLit(emailDom), sqlLit(keyName))
}

// buildBrokerTenantSQL reads the per-cluster broker tenant UUID — the value the
// proxy injects as x-queen-tenant, and therefore what a direct-to-broker run
// must send to land on the SAME broker-side rows.
func buildBrokerTenantSQL(idx []int, prefix string) string {
	var slugs []string
	for _, i := range idx {
		slugs = append(slugs, fmt.Sprintf("'%s-%04d'", sqlLit(prefix), i))
	}
	return fmt.Sprintf(`\set ON_ERROR_STOP on
SELECT slug || E'\t' || broker_tenant_uuid::text
FROM queen_proxy.clusters WHERE slug IN (%s) ORDER BY slug;
`, strings.Join(slugs, ", "))
}

func sqlLit(s string) string { return strings.ReplaceAll(s, "'", "''") }

func runPsql(cmdline, sql string) (string, error) {
	cmd := exec.Command("sh", "-c", cmdline)
	cmd.Stdin = strings.NewReader(sql)
	var sb strings.Builder
	cmd.Stdout = &sb
	cmd.Stderr = &sb
	err := cmd.Run()
	return sb.String(), err
}

func sortTenants(t []TenantCred) {
	for i := 1; i < len(t); i++ {
		for j := i; j > 0 && t[j].Idx < t[j-1].Idx; j-- {
			t[j], t[j-1] = t[j-1], t[j]
		}
	}
}
