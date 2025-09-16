package main

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// stateDB is the global DB used by demo/main. Declared here once.
var stateDB *StateDB

type StateDB struct {
	db   *leveldb.DB
	path string
	mu   sync.RWMutex
}

func NewStateDB(path string) (*StateDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	return &StateDB{db: db, path: path}, nil
}

func (s *StateDB) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

func (s *StateDB) Put(key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Put([]byte(key), value, nil)
}

func (s *StateDB) Get(key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return v, err
}

func (s *StateDB) PutState(key []byte, value []byte) error {
	ks := string(key)
	return s.Put(ks, value)
}

func (s *StateDB) GetState(key []byte) ([]byte, error) {
	ks := string(key)
	return s.Get(ks)
}

func (s *StateDB) DeleteState(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Delete([]byte(string(key)), nil)
}

// IterateAll materializes DB into memory. Prototype only.
func (s *StateDB) IterateAll(prefix string) (map[string][]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make(map[string][]byte)

	var iter iterator.Iterator
	if prefix == "" {
		iter = s.db.NewIterator(nil, nil)
	} else {
		iter = s.db.NewIterator(util.BytesPrefix([]byte(prefix)), nil)
	}
	defer iter.Release()

	for iter.Next() {
		k := string(iter.Key())
		v := iter.Value()
		c := make([]byte, len(v))
		copy(c, v)
		result[k] = c
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *StateDB) Commit(blockHeight uint64, root []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := fmt.Sprintf("block_%d_root", blockHeight)
	return s.db.Put([]byte(key), root, nil)
}

func (s *StateDB) GetRoot(blockHeight uint64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key := fmt.Sprintf("block_%d_root", blockHeight)
	v, err := s.db.Get([]byte(key), nil)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return v, err
}

// DumpToJSON now deterministic: sorted keys -> ordered array of {key,value}
func (s *StateDB) DumpToJSON(filename string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	iter := s.db.NewIterator(nil, nil)
	defer iter.Release()

	kvs := make(map[string]string)
	keys := make([]string, 0)
	for iter.Next() {
		k := string(iter.Key())
		v := iter.Value()
		kvs[k] = hex.EncodeToString(v)
		keys = append(keys, k)
	}
	if err := iter.Error(); err != nil {
		return err
	}
	sort.Strings(keys)

	ordered := make([]map[string]string, 0, len(keys))
	for _, k := range keys {
		ordered = append(ordered, map[string]string{"key": k, "value": kvs[k]})
	}

	data, err := json.MarshalIndent(ordered, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

func (s *StateDB) NewBatch() *leveldb.Batch {
	return new(leveldb.Batch)
}

func (s *StateDB) WriteBatch(b *leveldb.Batch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Write(b, nil)
}

func (s *StateDB) SavePersistentState(ps *PersistentState) error {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(ps); err != nil {
		return err
	}
	return s.Put("persistent_state", buf.Bytes())
}

func (s *StateDB) LoadPersistentState() (*PersistentState, error) {
	data, err := s.Get("persistent_state")
	if err != nil {
		return nil, err
	}
	if data == nil {
		return nil, fmt.Errorf("persistent_state not found")
	}
	dec := gob.NewDecoder(bytes.NewReader(data))
	var ps PersistentState
	if err := dec.Decode(&ps); err != nil {
		return nil, err
	}
	return &ps, nil
}
