package kms

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// VaultTransit uses the Transit API; Vault retains historical key versions, so
// old ciphertext remains decryptable after an operator rotates the key.
type VaultTransit struct {
	address, token, namespace, mount, key string
	client                                *http.Client
}

func newVaultTransitFromEnv() (*VaultTransit, error) {
	address := strings.TrimRight(strings.TrimSpace(os.Getenv("TASK_VAULT_ADDR")), "/")
	token, key := strings.TrimSpace(os.Getenv("TASK_VAULT_TOKEN")), strings.TrimSpace(os.Getenv("TASK_VAULT_TRANSIT_KEY"))
	if address == "" || token == "" || key == "" {
		return nil, fmt.Errorf("TASK_VAULT_ADDR, TASK_VAULT_TOKEN and TASK_VAULT_TRANSIT_KEY are required for Vault Transit")
	}
	if _, err := url.ParseRequestURI(address); err != nil {
		return nil, fmt.Errorf("TASK_VAULT_ADDR is invalid: %w", err)
	}
	mount := strings.Trim(strings.TrimSpace(os.Getenv("TASK_VAULT_TRANSIT_MOUNT")), "/")
	if mount == "" {
		mount = "transit"
	}
	return &VaultTransit{address: address, token: token, namespace: strings.TrimSpace(os.Getenv("TASK_VAULT_NAMESPACE")), mount: mount, key: key, client: &http.Client{Timeout: 10 * time.Second}}, nil
}
func (v *VaultTransit) ref() string { return "vault-transit:" + v.mount + ":" + v.key }
func (v *VaultTransit) Encrypt(ctx context.Context, plaintext, aad []byte) ([]byte, string, error) {
	var out struct {
		Data struct {
			Ciphertext string `json:"ciphertext"`
		} `json:"data"`
	}
	if err := v.call(ctx, "encrypt", map[string]string{"plaintext": base64.StdEncoding.EncodeToString(plaintext), "context": base64.StdEncoding.EncodeToString(aad)}, &out); err != nil {
		return nil, "", err
	}
	if out.Data.Ciphertext == "" {
		return nil, "", fmt.Errorf("Vault Transit returned empty ciphertext")
	}
	return []byte(out.Data.Ciphertext), v.ref(), nil
}
func (v *VaultTransit) Decrypt(ctx context.Context, ciphertext []byte, ref string, aad []byte) ([]byte, error) {
	if ref != "" && ref != v.ref() {
		return nil, fmt.Errorf("key reference %q is not configured", ref)
	}
	var out struct {
		Data struct {
			Plaintext string `json:"plaintext"`
		} `json:"data"`
	}
	if err := v.call(ctx, "decrypt", map[string]string{"ciphertext": string(ciphertext), "context": base64.StdEncoding.EncodeToString(aad)}, &out); err != nil {
		return nil, err
	}
	plain, err := base64.StdEncoding.DecodeString(out.Data.Plaintext)
	if err != nil {
		return nil, fmt.Errorf("decode Vault Transit plaintext: %w", err)
	}
	return plain, nil
}
func (v *VaultTransit) Rewrap(ctx context.Context, ciphertext []byte, ref string, aad []byte) ([]byte, string, error) {
	if ref != "" && ref != v.ref() {
		return nil, "", fmt.Errorf("key reference %q is not configured", ref)
	}
	var out struct {
		Data struct {
			Ciphertext string `json:"ciphertext"`
		} `json:"data"`
	}
	if err := v.call(ctx, "rewrap", map[string]string{"ciphertext": string(ciphertext), "context": base64.StdEncoding.EncodeToString(aad)}, &out); err != nil {
		return nil, "", err
	}
	if out.Data.Ciphertext == "" {
		return nil, "", fmt.Errorf("Vault Transit returned empty ciphertext")
	}
	return []byte(out.Data.Ciphertext), v.ref(), nil
}
func (v *VaultTransit) call(ctx context.Context, operation string, input any, out any) error {
	body, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, v.address+"/v1/"+v.mount+"/"+operation+"/"+v.key, bytes.NewReader(body))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Vault-Token", v.token)
	if v.namespace != "" {
		request.Header.Set("X-Vault-Namespace", v.namespace)
	}
	response, err := v.client.Do(request)
	if err != nil {
		return fmt.Errorf("Vault Transit %s: %w", operation, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode > 299 {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return fmt.Errorf("Vault Transit %s returned %s: %s", operation, response.Status, strings.TrimSpace(string(limited)))
	}
	if err = json.NewDecoder(response.Body).Decode(out); err != nil {
		return fmt.Errorf("decode Vault Transit %s response: %w", operation, err)
	}
	return nil
}
