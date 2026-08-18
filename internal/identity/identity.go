// Package identity defines the authentication boundary. HTTP header identity
// is a development adapter only; OIDC/JWT adapters can implement Provider
// without leaking transport-specific claims into application handlers.
package identity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/google/uuid"
)

type Subject struct {
	Actor    string
	TenantID uuid.UUID
	Role     string
}
type Provider interface {
	Authenticate(*http.Request) (Subject, error)
}

var ErrUnauthenticated = errors.New("unauthenticated")

type HeaderProvider struct{}

func (HeaderProvider) Authenticate(r *http.Request) (Subject, error) {
	actor := r.Header.Get("X-Actor-ID")
	tenant, err := uuid.Parse(r.Header.Get("X-Tenant-ID"))
	role := r.Header.Get("X-Role")
	if actor == "" || err != nil {
		return Subject{}, ErrUnauthenticated
	}
	return Subject{Actor: actor, TenantID: tenant, Role: role}, nil
}

// OIDCConfig deliberately maps only identity claims. Authorization always
// comes from tenant-scoped dynamic RBAC in PostgreSQL, never from a token role.
type OIDCConfig struct {
	Issuer      string
	Audience    string
	TenantClaim string
	RoleClaim   string
}

type OIDCProvider struct {
	verifier    *oidc.IDTokenVerifier
	tenantClaim string
	roleClaim   string
}

func NewOIDC(ctx context.Context, cfg OIDCConfig) (*OIDCProvider, error) {
	cfg.Issuer = strings.TrimSpace(cfg.Issuer)
	cfg.Audience = strings.TrimSpace(cfg.Audience)
	if cfg.Issuer == "" || cfg.Audience == "" {
		return nil, errors.New("OIDC issuer and audience are required")
	}
	if cfg.TenantClaim == "" {
		cfg.TenantClaim = "tenant_id"
	}
	if cfg.RoleClaim == "" {
		cfg.RoleClaim = "role"
	}
	provider, err := oidc.NewProvider(ctx, cfg.Issuer)
	if err != nil {
		return nil, fmt.Errorf("discover OIDC issuer: %w", err)
	}
	return &OIDCProvider{verifier: provider.Verifier(&oidc.Config{ClientID: cfg.Audience}), tenantClaim: cfg.TenantClaim, roleClaim: cfg.RoleClaim}, nil
}

func (p *OIDCProvider) Authenticate(r *http.Request) (Subject, error) {
	if p == nil || p.verifier == nil {
		return Subject{}, ErrUnauthenticated
	}
	header := strings.TrimSpace(r.Header.Get("Authorization"))
	parts := strings.Fields(header)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return Subject{}, ErrUnauthenticated
	}
	token, err := p.verifier.Verify(r.Context(), parts[1])
	if err != nil {
		return Subject{}, ErrUnauthenticated
	}
	var claims map[string]json.RawMessage
	if err = token.Claims(&claims); err != nil {
		return Subject{}, ErrUnauthenticated
	}
	tenantRaw, ok := claims[p.tenantClaim]
	if !ok {
		return Subject{}, ErrUnauthenticated
	}
	var tenantText string
	if err = json.Unmarshal(tenantRaw, &tenantText); err != nil {
		return Subject{}, ErrUnauthenticated
	}
	tenant, err := uuid.Parse(tenantText)
	if err != nil {
		return Subject{}, ErrUnauthenticated
	}
	role := ""
	if raw, exists := claims[p.roleClaim]; exists {
		_ = json.Unmarshal(raw, &role)
	}
	if token.Subject == "" {
		return Subject{}, ErrUnauthenticated
	}
	return Subject{Actor: token.Subject, TenantID: tenant, Role: role}, nil
}
