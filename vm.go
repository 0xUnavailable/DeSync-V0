package main

import (
	"bytes"
	"fmt"

)

// SimpleStateMachine: supports live DB or staging map mode.
type SimpleStateMachine struct {
	DB      *StateDB
	Staging map[string][]byte
}

func NewSimpleStateMachine(db *StateDB) *SimpleStateMachine {
	return &SimpleStateMachine{DB: db}
}

func NewStagingStateMachine(staging map[string][]byte) *SimpleStateMachine {
	return &SimpleStateMachine{Staging: staging}
}

func (s *SimpleStateMachine) Apply(cmd []byte) ([]byte, error) {
	parts := bytes.SplitN(cmd, []byte(":"), 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cmd format, expected key:value")
	}
	key := string(parts[0])
	value := parts[1]

	if s.Staging != nil {
		v := make([]byte, len(value))
		copy(v, value)
		s.Staging[key] = v
		return []byte("OK"), nil
	}

	if s.DB == nil {
		return nil, fmt.Errorf("stateDB not initialized")
	}
	return []byte("OK"), s.DB.Put(key, append([]byte(nil), value...))
}

