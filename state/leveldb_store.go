package state

import (
	"github.com/syndtr/goleveldb/leveldb"
	"path/filepath"
	"fmt"
)

type LevelDBStore struct {
	db *leveldb.DB
}

func NewLevelDBStore(path string) (*LevelDBStore, error) {
	p := filepath.Clean(path)
	db, err := leveldb.OpenFile(p, nil)
	if err != nil {
		return nil, fmt.Errorf("open leveldb %s: %w", p, err)
	}
	return &LevelDBStore{db: db}, nil
}

func (l *LevelDBStore) Get(key string) ([]byte, error) {
	b, err := l.db.Get([]byte(key), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	cp := make([]byte, len(b))
	copy(cp, b)
	return cp, nil
}

func (l *LevelDBStore) Set(key string, value []byte) error {
	cp := make([]byte, len(value))
	copy(cp, value)
	return l.db.Put([]byte(key), cp, nil)
}

func (l *LevelDBStore) Close() error {
	return l.db.Close()
}
