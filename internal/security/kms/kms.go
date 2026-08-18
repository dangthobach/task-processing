// Package kms provides envelope-style payload protection without coupling the
// task runtime to a particular cloud KMS. Production adapters can implement
// Protector; the local implementation is deliberately opt-in.
package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"
)

type Protector interface {
	Encrypt(context.Context, []byte, []byte) ([]byte, string, error)
	Decrypt(context.Context, []byte, string, []byte) ([]byte, error)
}

// Rewrapper rotates ciphertext to the provider's active key version without
// requiring callers to handle raw key material.
type Rewrapper interface {
	Rewrap(context.Context, []byte, string, []byte) ([]byte, string, error)
}

type Local struct {
	key []byte
	ref string
}

func FromEnv() (Protector, error) {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("TASK_KMS_PROVIDER")))
	if provider == "vault" || provider == "vault-transit" {
		return newVaultTransitFromEnv()
	}
	if provider != "" && provider != "local" {
		return nil, fmt.Errorf("unsupported TASK_KMS_PROVIDER %q", provider)
	}
	raw := os.Getenv("TASK_PAYLOAD_MASTER_KEY")
	if raw == "" {
		return nil, nil
	}
	key, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("TASK_PAYLOAD_MASTER_KEY must be base64: %w", err)
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("TASK_PAYLOAD_MASTER_KEY must decode to 32 bytes")
	}
	ref := os.Getenv("TASK_PAYLOAD_KEY_REF")
	if ref == "" {
		ref = "local:aes-256-gcm:v1"
	}
	return &Local{key: key, ref: ref}, nil
}

func (l *Local) Encrypt(_ context.Context, plaintext, aad []byte) ([]byte, string, error) {
	block, err := aes.NewCipher(l.key)
	if err != nil {
		return nil, "", err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, "", err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, "", err
	}
	return gcm.Seal(nonce, nonce, plaintext, aad), l.ref, nil
}
func (l *Local) Decrypt(_ context.Context, ciphertext []byte, ref string, aad []byte) ([]byte, error) {
	if ref != l.ref {
		return nil, fmt.Errorf("key reference %q is not available", ref)
	}
	block, err := aes.NewCipher(l.key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, fmt.Errorf("ciphertext is truncated")
	}
	return gcm.Open(nil, ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():], aad)
}

func (l *Local) Rewrap(ctx context.Context, ciphertext []byte, ref string, aad []byte) ([]byte, string, error) {
	plain, err := l.Decrypt(ctx, ciphertext, ref, aad)
	if err != nil {
		return nil, "", err
	}
	return l.Encrypt(ctx, plain, aad)
}
