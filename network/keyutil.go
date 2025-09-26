package network

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io/ioutil"
	"os"

	crypto "github.com/libp2p/go-libp2p/core/crypto"
)

// LoadOrCreatePrivKey loads an ed25519 key from file or generates one if missing.
// Returns a libp2p crypto.PrivKey.
func LoadOrCreatePrivKey(path string) (crypto.PrivKey, error) {
	if path == "" {
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		return priv, err
	}
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		priv, _, err := crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			return nil, err
		}
		b, err := crypto.MarshalPrivateKey(priv)
		if err != nil {
			return nil, err
		}
		if err := ioutil.WriteFile(path, []byte(hex.EncodeToString(b)), 0600); err != nil {
			return nil, err
		}
		return priv, nil
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	b, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, err
	}
	priv, err := crypto.UnmarshalPrivateKey(b)
	if err != nil {
		return nil, err
	}
	return priv, nil
}
