// Package identity defines the authentication boundary. HTTP header identity
// is a development adapter only; OIDC/JWT adapters can implement Provider
// without leaking transport-specific claims into application handlers.
package identity

import (
	"errors"
	"net/http"

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
