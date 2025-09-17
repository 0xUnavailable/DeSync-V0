package desync

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"sync"
)

// LevelDB driver
import (
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/util"
)

// ---------------------------
// StateDB with integrated SMT
// ---------------------------

// Global stateDB instance (same as before)
var stateDB *StateDB

type StateDB struct {
	db   *leveldb.DB
	path string
	mu   sync.RWMutex

	// SMT integration
	smt   *SparseMerkleTree
	depth int
}

func NewStateDB(path string) (*StateDB, error) {
	db, err := leveldb.OpenFile(path, nil)
	if err != nil {
		return nil, err
	}
	s := &StateDB{db: db, path: path, depth: SMTDepth}
	// Create SMT (it will use this StateDB's Get/Put/WriteBatch)
	smt, err := NewSparseMerkleTree(s, s.depth)
	if err != nil {
		// close DB on failure
		_ = db.Close()
		return nil, err
	}
	s.smt = smt
	return s, nil
}

func (s *StateDB) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// ---------------------------
// Basic LevelDB wrappers (unchanged semantics)
// ---------------------------

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

func (s *StateDB) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Delete([]byte(key), nil)
}

// PutState writes the key->value to the DB AND updates the SMT (incremental).
// This keeps SMT root in-sync with persisted KV writes. This is the integrated behavior.
func (s *StateDB) PutState(key []byte, value []byte) error {
	// 1) Persist the raw KV under user key
	if err := s.Put(string(key), append([]byte(nil), value...)); err != nil {
		return err
	}
	// 2) Update SMT keyed on the raw key bytes
	_, err := s.smt.Update(key, value)
	return err
}

func (s *StateDB) GetState(key []byte) ([]byte, error) {
	return s.Get(string(key))
}

func (s *StateDB) DeleteState(key []byte) error {
	// Deleting in SMT is not implemented in this simple prototype as structural removal;
	// instead we write an empty value (zero) to represent deletion in the SMT.
	// Persist zero value to DB and update SMT with zero.
	zero := []byte{}
	if err := s.Put(string(key), zero); err != nil {
		return err
	}
	_, err := s.smt.Update(key, zero)
	return err
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
	// store commit root under deterministic key
	return s.Put(fmt.Sprintf("block_%d_root", blockHeight), root)
}

func (s *StateDB) GetRoot(blockHeight uint64) ([]byte, error) {
	return s.Get(fmt.Sprintf("block_%d_root", blockHeight))
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

// ---------------------------
// SMT helpers on StateDB
// ---------------------------

// CurrentRoot returns the SMT root persisted in DB (or zero root if none).
func (s *StateDB) CurrentRoot() ([]byte, error) {
	return s.smt.GetRoot()
}

// SMTUpdate updates key->value in SMT and persists node updates via batch write.
// This duplicates PutState behavior but exposed for direct SMT-only updates.
func (s *StateDB) SMTUpdate(key []byte, value []byte) ([]byte, error) {
	// Persist the raw KV also so application-level values are stored
	if err := s.Put(string(key), append([]byte(nil), value...)); err != nil {
		return nil, err
	}
	return s.smt.Update(key, value)
}

// SMTGenerateProof returns sibling hashes bottom-up for the given key.
func (s *StateDB) SMTGenerateProof(key []byte) ([][]byte, error) {
	return s.smt.GenerateProof(key)
}

// SMTDumpNodes returns SMT node key->hex for debug
func (s *StateDB) SMTDumpNodes() (map[string]string, error) {
	return s.smt.DumpNodeHex()
}

