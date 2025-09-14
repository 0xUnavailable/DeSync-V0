package main

import (
	"bytes"
	"testing"

	"github.com/ProtonMail/go-ecvrf/ecvrf"
)

func TestVRFProveVerify(t *testing.T) {
    sk, err := ecvrf.GenerateKey(nil)
    if err != nil {
        t.Fatalf("generate key err: %v", err)
    }
    pk, err := sk.Public()
    if err != nil {
        t.Fatalf("get public err: %v", err)
    }

    seed := []byte("epoch-seed-12345")

    beta, proof, err := sk.Prove(seed)
    if err != nil {
        t.Fatalf("prove err: %v", err)
    }

    ok, beta2, err := pk.Verify(seed, proof)
    if err != nil {
        t.Fatalf("verify err: %v", err)
    }
    if !ok {
        t.Fatal("verify failed")
    }
    if !bytes.Equal(beta, beta2) {
        t.Fatalf("beta mismatch: got %x want %x", beta2, beta)
    }

    // test wrappers
    h, p, err := vrfGenerateHash(seed, sk.Bytes())
    if err != nil {
        t.Fatalf("vrfGenerateHash err: %v", err)
    }

    ok2, err := vrfVerifyHash(h, p, pk.Bytes(), seed)
    if err != nil {
        t.Fatalf("vrfVerifyHash err: %v", err)
    }
    if !ok2 {
        t.Fatal("vrfVerifyHash returned false")
    }
}
