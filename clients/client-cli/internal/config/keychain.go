package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/zalando/go-keyring"
)

const keychainService = "queenctl"

// readToken resolves a token-ref string to its plaintext value.
//
// Schemes:
//   - ""                     -> no token (anonymous)
//   - "env"                  -> read QUEEN_TOKEN at runtime
//   - "keychain://<id>"      -> OS keychain via go-keyring
//   - "file:///abs/path"     -> trim space, return file contents
//   - "literal:<token>"      -> inline (CI escape hatch, discouraged)
func readToken(ref, contextName string) (string, error) {
	if ref == "" {
		return "", nil
	}
	switch {
	case ref == "env":
		return strings.TrimSpace(os.Getenv("QUEEN_TOKEN")), nil
	case strings.HasPrefix(ref, "keychain://"):
		id := strings.TrimPrefix(ref, "keychain://")
		v, err := keyring.Get(keychainService, id)
		if err != nil {
			return "", fmt.Errorf("keychain (%s): %w", id, err)
		}
		return v, nil
	case strings.HasPrefix(ref, "file://"):
		path := strings.TrimPrefix(ref, "file://")
		b, err := os.ReadFile(path)
		if err != nil {
			return "", fmt.Errorf("token file %s: %w", path, err)
		}
		return strings.TrimSpace(string(b)), nil
	case strings.HasPrefix(ref, "literal:"):
		return strings.TrimPrefix(ref, "literal:"), nil
	default:
		// Backwards compat: bare value treated as literal.
		return ref, nil
	}
}

// StoreToken writes token under id in the OS keychain. Returns the ref string
// to persist in the config file.
func StoreToken(id, token string) (string, error) {
	if err := keyring.Set(keychainService, id, token); err != nil {
		return "", fmt.Errorf("keychain set: %w", err)
	}
	return "keychain://" + id, nil
}

// DeleteToken removes the token from the OS keychain.
func DeleteToken(id string) error {
	err := keyring.Delete(keychainService, id)
	if err != nil && !errors.Is(err, keyring.ErrNotFound) {
		return fmt.Errorf("keychain delete: %w", err)
	}
	return nil
}
