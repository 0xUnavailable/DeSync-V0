package state

// KeyValueStore defines the minimal DB interface for SMT.
type KeyValueStore interface {
	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
}

// memKV is an in-memory implementation of KeyValueStore (for dev + tests).
type memKV struct {
	m map[string][]byte
}

func NewMemKV() *memKV {
	return &memKV{m: make(map[string][]byte)}
}

func (m *memKV) Get(k string) ([]byte, error) {
	if v, ok := m.m[k]; ok {
		// return a copy
		cp := make([]byte, len(v))
		copy(cp, v)
		return cp, nil
	}
	return nil, nil
}

func (m *memKV) Set(k string, v []byte) error {
	cp := make([]byte, len(v))
	copy(cp, v)
	m.m[k] = cp
	return nil
}
