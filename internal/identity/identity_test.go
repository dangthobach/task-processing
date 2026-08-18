package identity

import (
	"context"
	"net/http/httptest"
	"testing"
)

func TestHeaderProviderRejectsIncompleteIdentity(t *testing.T) {
	request := httptest.NewRequest("GET", "/", nil)
	if _, err := (HeaderProvider{}).Authenticate(request); err != ErrUnauthenticated {
		t.Fatalf("err=%v", err)
	}
}
func TestOIDCRequiresIssuerAndAudience(t *testing.T) {
	if _, err := NewOIDC(context.Background(), OIDCConfig{}); err == nil {
		t.Fatal("expected config error")
	}
}
