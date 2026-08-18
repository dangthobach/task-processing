package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHealthDoesNotRequireIdentity(t *testing.T) {
	r := (&API{Events: NewEventHub()}).Router()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)
	if res.Code != http.StatusOK {
		t.Fatalf("status = %d", res.Code)
	}
	var body struct {
		Data map[string]string `json:"data"`
		Meta RequestMeta       `json:"meta"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data["status"] != "ok" || body.Meta.RequestID == "" || body.Meta.TraceID == "" {
		t.Fatalf("unexpected envelope: %+v", body)
	}
	if res.Header().Get("X-Request-ID") != body.Meta.RequestID {
		t.Fatal("response correlation header missing")
	}
}
func TestProtectedEndpointRejectsAnonymous(t *testing.T) {
	r := (&API{Events: NewEventHub()}).Router()
	req := httptest.NewRequest(http.MethodGet, "/api/v1/job-runs?project_id=00000000-0000-0000-0000-000000000000", nil)
	res := httptest.NewRecorder()
	r.ServeHTTP(res, req)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d", res.Code)
	}
	var body struct {
		Error struct {
			Code string `json:"code"`
		} `json:"error"`
		Meta RequestMeta `json:"meta"`
	}
	if err := json.Unmarshal(res.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != "UNAUTHENTICATED" || body.Meta.TraceID == "" {
		t.Fatalf("unexpected problem: %+v", body)
	}
}
