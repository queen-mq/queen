// Package config loads and persists the queenctl per-user config:
//
//	~/.queen/config.yaml
//
// kubectl-style multi-context shape, with env vars (QUEEN_SERVER, QUEEN_TOKEN,
// QUEEN_CONTEXT) taking precedence over the on-disk value.
package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

const (
	envServer  = "QUEEN_SERVER"
	envToken   = "QUEEN_TOKEN"
	envContext = "QUEEN_CONTEXT"
)

// Context describes a single Queen endpoint plus optional credential pointer.
type Context struct {
	Name string `yaml:"name"`
	// Server is the broker base URL (no trailing /api/v1).
	Server string `yaml:"server"`
	// TokenRef is one of:
	//   - "" (no token)
	//   - "env"            (read from QUEEN_TOKEN at runtime)
	//   - "keychain://<id>"
	//   - "file:///abs/path"
	//   - "literal:<token>" (for CI - not recommended)
	TokenRef string `yaml:"token-ref,omitempty"`
	// Insecure disables TLS verification (test/dev only).
	Insecure bool `yaml:"insecure,omitempty"`
}

// File is the on-disk shape of ~/.queen/config.yaml.
type File struct {
	CurrentContext string    `yaml:"current-context,omitempty"`
	Contexts       []Context `yaml:"contexts,omitempty"`
}

// Resolved is the effective context for the current invocation, after env
// overrides and explicit --flag overrides have been applied.
type Resolved struct {
	Name     string
	Server   string
	Token    string
	Insecure bool
}

// DefaultPath returns the path of the queenctl config file.
func DefaultPath() (string, error) {
	if p := os.Getenv("QUEEN_CONFIG"); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".queen", "config.yaml"), nil
}

// Load reads the config at path. A missing file is not an error: Load
// returns an empty File so first-run users get the env-only experience.
func Load(path string) (*File, error) {
	if path == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return nil, err
		}
	}
	b, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return &File{}, nil
		}
		return nil, err
	}
	var f File
	if err := yaml.Unmarshal(b, &f); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return &f, nil
}

// Save atomically writes the config to path, creating parent directories.
func Save(path string, f *File) error {
	if path == "" {
		var err error
		path, err = DefaultPath()
		if err != nil {
			return err
		}
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return err
	}
	b, err := yaml.Marshal(f)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// FindContext returns the named context or nil.
func (f *File) FindContext(name string) *Context {
	if f == nil {
		return nil
	}
	for i := range f.Contexts {
		if f.Contexts[i].Name == name {
			return &f.Contexts[i]
		}
	}
	return nil
}

// SetContext upserts a context.
func (f *File) SetContext(c Context) {
	for i := range f.Contexts {
		if f.Contexts[i].Name == c.Name {
			f.Contexts[i] = c
			return
		}
	}
	f.Contexts = append(f.Contexts, c)
}

// DeleteContext removes a context by name. Returns true if removed.
func (f *File) DeleteContext(name string) bool {
	for i := range f.Contexts {
		if f.Contexts[i].Name == name {
			f.Contexts = append(f.Contexts[:i], f.Contexts[i+1:]...)
			if f.CurrentContext == name {
				f.CurrentContext = ""
			}
			return true
		}
	}
	return false
}

// Overrides bundles flag-level overrides applied on top of the loaded file.
type Overrides struct {
	Context  string
	Server   string
	Token    string
	Insecure *bool
}

// Resolve applies env vars + overrides onto f and returns the effective
// connection for this invocation. The returned Resolved.Server is always
// populated when err == nil.
func Resolve(f *File, o Overrides) (*Resolved, error) {
	if f == nil {
		f = &File{}
	}
	r := &Resolved{}

	// Pick the context name.
	r.Name = o.Context
	if r.Name == "" {
		r.Name = os.Getenv(envContext)
	}
	if r.Name == "" {
		r.Name = f.CurrentContext
	}

	if c := f.FindContext(r.Name); c != nil {
		r.Server = c.Server
		r.Insecure = c.Insecure
		t, err := readToken(c.TokenRef, c.Name)
		if err != nil {
			return nil, err
		}
		r.Token = t
	}

	// Apply explicit overrides last (highest precedence).
	if v := os.Getenv(envServer); v != "" {
		r.Server = v
	}
	if o.Server != "" {
		r.Server = o.Server
	}
	if v := os.Getenv(envToken); v != "" {
		r.Token = v
	}
	if o.Token != "" {
		r.Token = o.Token
	}
	if o.Insecure != nil {
		r.Insecure = *o.Insecure
	}

	if r.Server == "" {
		return nil, fmt.Errorf("no server configured: set --server, %s, or run 'queenctl config set-context'", envServer)
	}
	return r, nil
}
