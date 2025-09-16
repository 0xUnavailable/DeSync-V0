package main

import (
	"bytes"

	"github.com/ProtonMail/go-ecvrf/ecvrf"
)

func vrfGenerateHash(seed []byte, privateKeyBytes []byte) ([]byte, []byte, error) {
	sk, err := ecvrf.NewPrivateKey(privateKeyBytes)
	if err != nil {
		return nil, nil, err
	}
	beta, proof, err := sk.Prove(seed)
	if err != nil {
		return nil, nil, err
	}
	return beta, proof, nil
}

func vrfVerifyHash(hash []byte, proof []byte, publicKeyBytes []byte, seed []byte) (bool, error) {
	pk, err := ecvrf.NewPublicKey(publicKeyBytes)
	if err != nil {
		return false, err
	}
	ok, beta2, err := pk.Verify(seed, proof)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return bytes.Equal(beta2, hash), nil
}
