// Copyright © 2026 Ory Corp
// SPDX-License-Identifier: Apache-2.0

package relationtuple

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"

	"github.com/gofrs/uuid"
)

// BatchTokenPrefix versions the opaque token format.
const BatchTokenPrefix = "kbt1."

// tokenPayload is the encrypted, authenticated content of a version token.
// It never contains namespace/object/subject values: only random row UUIDs
// and a short identity hash. Tokens are opaque and non-reversible; they are
// optimistic-concurrency cursors, never credentials (authorization is always
// re-evaluated on every request).
type tokenPayload struct {
	Absent  bool   `json:"a,omitempty"`
	NID     string `json:"n,omitempty"`
	Shard   string `json:"s,omitempty"`
	KeyHash string `json:"k,omitempty"`
}

// BatchTokenCodec mints and verifies opaque batch version tokens.
type BatchTokenCodec struct {
	gcm cipher.AEAD
}

// NewBatchTokenCodec derives a stable encryption key from the configured
// secret. The empty secret still yields a working key (tests/default
// deployments); production multi-instance deployments should set the batch
// token secret so every instance shares it.
func NewBatchTokenCodec(secret string) *BatchTokenCodec {
	sum := sha256.Sum256([]byte("keto-batch-version-token-v1:" + secret))
	block, err := aes.NewCipher(sum[:])
	if err != nil {
		// sha256 sum is always a valid AES-256 key; this is unreachable.
		panic(err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		panic(err)
	}
	return &BatchTokenCodec{gcm: gcm}
}

// MintPresent creates a token bound to one stored row version (shard id).
func (c *BatchTokenCodec) MintPresent(nid uuid.UUID, key TupleKey, shard uuid.UUID) string {
	return c.mint(tokenPayload{
		NID:     nid.String(),
		Shard:   shard.String(),
		KeyHash: key.KeyHash(),
	})
}

// MintAbsent creates a token proving that, at mint time, the relation did not
// exist. It is bound to the relation identity.
func (c *BatchTokenCodec) MintAbsent(nid uuid.UUID, key TupleKey) string {
	return c.mint(tokenPayload{
		Absent:  true,
		NID:     nid.String(),
		KeyHash: key.KeyHash(),
	})
}

func (c *BatchTokenCodec) mint(p tokenPayload) string {
	plain, err := json.Marshal(p)
	if err != nil {
		panic(err)
	}
	nonce := make([]byte, c.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		panic(err)
	}
	sealed := c.gcm.Seal(nonce, nonce, plain, nil)
	return BatchTokenPrefix + base64.RawURLEncoding.EncodeToString(sealed)
}

// Parse validates and decrypts a token. Any malformed, tampered or foreign-key
// token returns an error; callers map that to a version conflict.
func (c *BatchTokenCodec) Parse(token string) (*tokenPayload, error) {
	if len(token) <= len(BatchTokenPrefix) || token[:len(BatchTokenPrefix)] != BatchTokenPrefix {
		return nil, errors.New("malformed batch version token")
	}
	raw, err := base64.RawURLEncoding.DecodeString(token[len(BatchTokenPrefix):])
	if err != nil {
		return nil, errors.New("malformed batch version token encoding")
	}
	ns := c.gcm.NonceSize()
	if len(raw) < ns+1 {
		return nil, errors.New("malformed batch version token length")
	}
	nonce, ciphertext := raw[:ns], raw[ns:]
	plain, err := c.gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, errors.New("batch version token authentication failed")
	}
	var p tokenPayload
	if err := json.Unmarshal(plain, &p); err != nil {
		return nil, errors.New("malformed batch version token payload")
	}
	return &p, nil
}

// verifyExpectedToken checks an item-supplied token against the current
// in-planner state. It returns false when the token does not match.
func (c *BatchTokenCodec) verifyExpectedToken(token string, nid uuid.UUID, key TupleKey, st *planKeyState) bool {
	if token == "" {
		return true
	}
	p, err := c.Parse(token)
	if err != nil {
		return false
	}
	if p.NID != nid.String() || p.KeyHash != key.KeyHash() {
		return false
	}
	if p.Absent {
		return !st.exists
	}
	return st.exists && !st.shard.IsNil() && p.Shard == st.shard.String()
}
