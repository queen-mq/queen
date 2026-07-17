package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
)

// AES-256-GCM payload encryption, matching the C++ EncryptionService wire format
// ({encrypted, iv, authTag} base64). Enabled when QUEEN_ENCRYPTION_KEY (64 hex
// chars = 32 bytes) is set. This adds real per-message crypto CPU to push (seal)
// and pop (open) — exactly the feature cost we want to measure.
type Crypto struct {
	gcm cipher.AEAD
}

func NewCrypto(hexKey string) *Crypto {
	if len(hexKey) != 64 {
		return nil
	}
	key, err := hex.DecodeString(hexKey)
	if err != nil || len(key) != 32 {
		return nil
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil
	}
	g, err := cipher.NewGCM(block)
	if err != nil {
		return nil
	}
	return &Crypto{gcm: g}
}

// encryptPayload seals plaintext and returns the {encrypted,iv,authTag} object
// bytes (base64 fields), as the C++ push path stores it.
func (c *Crypto) encryptPayload(plaintext []byte) []byte {
	iv := make([]byte, c.gcm.NonceSize())
	_, _ = rand.Read(iv)
	sealed := c.gcm.Seal(nil, iv, plaintext, nil)
	// GCM appends the 16-byte tag; split it out to match the C++ shape.
	tag := sealed[len(sealed)-16:]
	ct := sealed[:len(sealed)-16]
	obj := map[string]string{
		"encrypted": base64.StdEncoding.EncodeToString(ct),
		"iv":        base64.StdEncoding.EncodeToString(iv),
		"authTag":   base64.StdEncoding.EncodeToString(tag),
	}
	b, _ := json.Marshal(obj)
	return b
}

// decryptField opens a base64 {encrypted,iv,authTag} back to plaintext bytes.
func (c *Crypto) decrypt(encB64, ivB64, tagB64 string) ([]byte, bool) {
	ct, e1 := base64.StdEncoding.DecodeString(encB64)
	iv, e2 := base64.StdEncoding.DecodeString(ivB64)
	tag, e3 := base64.StdEncoding.DecodeString(tagB64)
	if e1 != nil || e2 != nil || e3 != nil {
		return nil, false
	}
	sealed := append(ct, tag...)
	pt, err := c.gcm.Open(nil, iv, sealed, nil)
	if err != nil {
		return nil, false
	}
	return pt, true
}
