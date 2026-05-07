package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	f := &File{
		CurrentContext: "dev",
		Contexts: []Context{
			{Name: "dev", Server: "http://localhost:6632", TokenRef: "literal:abc"},
			{Name: "prod", Server: "https://prod.queen", TokenRef: "env"},
		},
	}
	if err := Save(path, f); err != nil {
		t.Fatal(err)
	}
	got, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.CurrentContext != "dev" || len(got.Contexts) != 2 {
		t.Errorf("unexpected: %+v", got)
	}
	if c := got.FindContext("prod"); c == nil || c.Server != "https://prod.queen" {
		t.Errorf("missing prod context")
	}
}

func TestResolvePrecedence(t *testing.T) {
	// File only.
	f := &File{
		CurrentContext: "dev",
		Contexts:       []Context{{Name: "dev", Server: "http://file"}},
	}
	r, err := Resolve(f, Overrides{})
	if err != nil || r.Server != "http://file" {
		t.Fatalf("file precedence failed: %+v %v", r, err)
	}
	// Env beats file.
	t.Setenv("QUEEN_SERVER", "http://env")
	r, _ = Resolve(f, Overrides{})
	if r.Server != "http://env" {
		t.Errorf("env should beat file: %s", r.Server)
	}
	// Flag beats env.
	r, _ = Resolve(f, Overrides{Server: "http://flag"})
	if r.Server != "http://flag" {
		t.Errorf("flag should beat env: %s", r.Server)
	}
}

func TestResolveMissingServer(t *testing.T) {
	f := &File{}
	if _, err := Resolve(f, Overrides{}); err == nil {
		t.Error("expected error when no server is configured")
	}
}

func TestDeleteContext(t *testing.T) {
	f := &File{
		CurrentContext: "dev",
		Contexts: []Context{
			{Name: "dev"},
			{Name: "prod"},
		},
	}
	if !f.DeleteContext("dev") {
		t.Fatal("expected to delete")
	}
	if f.CurrentContext != "" || len(f.Contexts) != 1 {
		t.Errorf("unexpected: %+v", f)
	}
}

func TestLoadMissingIsEmpty(t *testing.T) {
	dir := t.TempDir()
	got, err := Load(filepath.Join(dir, "nope.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || len(got.Contexts) != 0 {
		t.Errorf("expected empty, got %+v", got)
	}
}

func TestReadTokenSchemes(t *testing.T) {
	t.Setenv("QUEEN_TOKEN", "from-env")
	v, err := readToken("env", "any")
	if err != nil || v != "from-env" {
		t.Errorf("env: got %q err=%v", v, err)
	}
	v, err = readToken("literal:abc", "any")
	if err != nil || v != "abc" {
		t.Errorf("literal: got %q err=%v", v, err)
	}
	v, err = readToken("", "any")
	if err != nil || v != "" {
		t.Errorf("empty: got %q err=%v", v, err)
	}
	tmp := filepath.Join(t.TempDir(), "tok")
	_ = os.WriteFile(tmp, []byte("file-token\n"), 0o600)
	v, err = readToken("file://"+tmp, "any")
	if err != nil || v != "file-token" {
		t.Errorf("file: got %q err=%v", v, err)
	}
}
