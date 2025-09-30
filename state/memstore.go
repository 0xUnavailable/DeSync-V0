package state

import (
	"sync"
)

// MemStore is a simple in-memory key-value store.
type MemStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewMemStore() *MemStore {
	return &MemStore{
		data: make(map[string][]byte),
	}
}

// Add to state/memstore.go
func (m *MemStore) SetBatch(updates map[string][]byte) {
    m.mu.Lock()
    defer m.mu.Unlock()
    for k, v := range updates {
        m.data[k] = v
    }
}


func (m *MemStore) Get(key []byte) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.data[string(key)]
	return val, ok
}

func (m *MemStore) Set(key []byte, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[string(key)] = val
}

// GetSnapshot returns a full copy of the current state as a map.
func (m *MemStore) GetSnapshot() map[string][]byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	snap := make(map[string][]byte, len(m.data))
	for k, v := range m.data {
		// copy each value to avoid aliasing
		buf := make([]byte, len(v))
		copy(buf, v)
		snap[k] = buf
	}
	return snap
}
