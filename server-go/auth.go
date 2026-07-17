package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
)

// Minimal HS256 JWT machinery, matching the per-request auth work the C++ broker
// does (jwt_validator.cpp): verify HMAC-SHA256 signature + decode & check claims.
// Enabled when QUEEN_JWT_SECRET is set. Since the goload loader sends no token,
// we verify a server-built token on every request to measure the CPU cost the
// auth path adds (representative of real per-request validation).
type Auth struct {
	secret []byte
	token  string
}

func NewAuth(secret string) *Auth {
	if secret == "" {
		return nil
	}
	a := &Auth{secret: []byte(secret)}
	a.token = a.build()
	return a
}

func (a *Auth) build() string {
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]interface{}{
		"sub":   "bench-producer",
		"scope": "read_write",
		"exp":   time.Now().Add(time.Hour).Unix(),
		"iat":   time.Now().Unix(),
	})
	payload := base64.RawURLEncoding.EncodeToString(claims)
	signing := hdr + "." + payload
	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(signing))
	sig := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	return signing + "." + sig
}

// verify does the full per-request cost: split, HMAC-SHA256, constant-time
// compare, base64-decode claims, JSON-parse, exp check.
func (a *Auth) verify(token string) bool {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return false
	}
	mac := hmac.New(sha256.New, a.secret)
	mac.Write([]byte(parts[0] + "." + parts[1]))
	expected := base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(parts[2])) {
		return false
	}
	claimsRaw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return false
	}
	var claims struct {
		Exp int64  `json:"exp"`
		Sub string `json:"sub"`
	}
	if json.Unmarshal(claimsRaw, &claims) != nil {
		return false
	}
	return claims.Exp > time.Now().Unix()
}

// check runs the representative per-request validation (of the server-built
// token). Returns true (authorized) for the benchmark.
func (a *Auth) check() bool {
	return a.verify(a.token)
}
