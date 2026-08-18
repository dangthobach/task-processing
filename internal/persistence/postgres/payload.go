package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/google/uuid"
)

func (s *Store) sealPayload(ctx context.Context, project, definition uuid.UUID, payload json.RawMessage) (json.RawMessage, []byte, string, error) {
	if s.PayloadProtector == nil {
		return payload, nil, "", nil
	}
	ciphertext, ref, err := s.PayloadProtector.Encrypt(ctx, payload, []byte(project.String()+":"+definition.String()))
	if err != nil {
		return nil, nil, "", fmt.Errorf("encrypt payload: %w", err)
	}
	return nil, ciphertext, ref, nil
}
func (s *Store) openPayload(ctx context.Context, project, definition uuid.UUID, plain json.RawMessage, ciphertext []byte, ref string) (json.RawMessage, error) {
	if len(ciphertext) == 0 {
		return plain, nil
	}
	if s.PayloadProtector == nil {
		return nil, fmt.Errorf("encrypted payload requires a configured KMS provider")
	}
	value, err := s.PayloadProtector.Decrypt(ctx, ciphertext, ref, []byte(project.String()+":"+definition.String()))
	if err != nil {
		return nil, fmt.Errorf("decrypt payload: %w", err)
	}
	if !json.Valid(value) {
		return nil, fmt.Errorf("decrypted payload is not JSON")
	}
	return json.RawMessage(value), nil
}
