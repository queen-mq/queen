package cmd

import (
	"strings"
	"testing"
)

// The site moved from flat .html pages at the root to sections, and every URL
// this command prints had to move with it. The old ones are not 404s (the site
// ships a 301 table for them) but they are one redirect hop each, and a
// redirect table is a shim for already-indexed URLs rather than something a
// binary should be shipping links into.

func TestDocsLinksAreCanonicalSectionURLs(t *testing.T) {
	for _, l := range docsLinks {
		if strings.HasSuffix(l.URL, ".html") {
			t.Errorf("%s: %s is a flat URL from the old site layout", l.Title, l.URL)
		}
		if !strings.HasPrefix(l.URL, docsSite+"/") {
			t.Errorf("%s: %s is not on %s", l.Title, l.URL, docsSite)
		}
		if !strings.HasSuffix(l.URL, "/") {
			t.Errorf("%s: %s has no trailing slash, which costs the reader a 307", l.Title, l.URL)
		}
	}
}

func TestDocsTopicsResolveToSectionPaths(t *testing.T) {
	for topic, path := range docsTopics {
		if !strings.HasPrefix(path, "/") || !strings.HasSuffix(path, "/") {
			t.Errorf("topic %q: path %q must be /section/ style", topic, path)
		}
		if strings.Contains(path, ".html") {
			t.Errorf("topic %q: path %q is a flat URL from the old site layout", topic, path)
		}
	}
}

func TestDocsTargetResolution(t *testing.T) {
	cases := []struct{ topic, want string }{
		// Every topic name the flat site published still resolves, now onto
		// the page that content moved to.
		{"quickstart", "https://queenmq.com/start/quickstart/"},
		{"concepts", "https://queenmq.com/use/model/"},
		{"architecture", "https://queenmq.com/internals/"},
		{"http-api", "https://queenmq.com/reference/http/"},
		{"clients", "https://queenmq.com/use/"},
		{"server", "https://queenmq.com/deploy/"},
		{"dashboard", "https://queenmq.com/deploy/dashboard/"},
		{"benchmarks", "https://queenmq.com/benchmarks/"},
		{"sizing", "https://queenmq.com/deploy/postgres/"},
		// The .html suffix the old flag taught people to type is tolerated.
		{"quickstart.html", "https://queenmq.com/start/quickstart/"},
		{"HTTP-API", "https://queenmq.com/reference/http/"},
		// Anything unlisted is a site path, so the pages this file does not
		// enumerate stay reachable.
		{"reference/http/pop", "https://queenmq.com/reference/http/pop/"},
		{"/internals/hotlist/", "https://queenmq.com/internals/hotlist/"},
		{"", "https://queenmq.com/"},
	}
	for _, c := range cases {
		if got := docsTarget(c.topic); got != c.want {
			t.Errorf("docsTarget(%q) = %q, want %q", c.topic, got, c.want)
		}
	}
}
