package store

import (
	"context"
	"sync"

	"anti-scraping-pipeline/internal/detect"
)

// Memory is an in-process Store for tests and offline runs.
type Memory struct {
	mu     sync.Mutex
	states map[string]*detect.UserState
}

func NewMemory() *Memory {
	return &Memory{states: map[string]*detect.UserState{}}
}

// Load returns copies, so callers can modify them freely.
func (m *Memory) Load(_ context.Context, users []string) (map[string]*detect.UserState, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	states := make(map[string]*detect.UserState, len(users))
	for _, u := range users {
		st := &detect.UserState{}
		if cur, ok := m.states[u]; ok {
			st.Session = cur.Session
			st.Window.Items = append([]detect.Activity(nil), cur.Window.Items...)
		}
		states[u] = st
	}
	return states, nil
}

func (m *Memory) Save(_ context.Context, updates []Update) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, up := range updates {
		st, ok := m.states[up.User]
		if !ok {
			st = &detect.UserState{}
			m.states[up.User] = st
		}
		st.Session = up.Session
		for _, a := range up.Added {
			st.Window.Add(a)
		}
	}
	return nil
}

func (m *Memory) put(user string, st *detect.UserState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[user] = st
}

func (m *Memory) remove(users []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, u := range users {
		delete(m.states, u)
	}
}
