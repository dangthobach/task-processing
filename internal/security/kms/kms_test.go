package kms

import (
	"context"
	"testing"
)

func TestLocalRoundTripAndAAD(t *testing.T) {
	p := &Local{key: make([]byte, 32), ref: "test"}
	sealed, ref, err := p.Encrypt(context.Background(), []byte("payload"), []byte("project/run"))
	if err != nil {
		t.Fatal(err)
	}
	plain, err := p.Decrypt(context.Background(), sealed, ref, []byte("project/run"))
	if err != nil || string(plain) != "payload" {
		t.Fatalf("decrypt = %q, %v", plain, err)
	}
	if _, err = p.Decrypt(context.Background(), sealed, ref, []byte("other")); err == nil {
		t.Fatal("AAD mismatch must fail")
	}
}
