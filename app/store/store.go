package store

import (
	"time"
)

type Store interface {
	Set(key, value string, expiryMillis ...int)
	Get(key string) (string, bool)
	Delete(key string)
}

type inMemoryStore struct {
	data map[string]*Entry
}

func NewInMemoryStore() Store {
	return &inMemoryStore{
		data: make(map[string]*Entry),
	}
}

// Helper to get current time
func now() time.Time {
	return time.Now()
}

// deleteExpired — check and delete key if expired
func (s *inMemoryStore) deleteExpired(key string, e *Entry) bool {
	// returns true if expired & deleted
	var expiry *time.Time
	if e.Str != nil {
		expiry = e.Str.Expiry
	} else if e.Lst != nil {
		expiry = e.Lst.Expiry
	}
	if expiry != nil && now().After(*expiry) {
		delete(s.data, key)
		return true
	}
	return false
}

// Set implementation
func (s *inMemoryStore) Set(key, value string, expiryMillis ...int) {
	var expiry *time.Time
	if len(expiryMillis) > 0 && expiryMillis[0] > 0 {
		t := now().Add(time.Duration(expiryMillis[0]) * time.Millisecond)
		expiry = &t
	}
	s.data[key] = &Entry{
		Str: &Item{Value: value, Expiry: expiry},
		Lst: nil,
	}
}

// Get implementation
func (s *inMemoryStore) Get(key string) (string, bool) {
	e, ok := s.data[key]
	if !ok {
		return "", false
	}
	if s.deleteExpired(key, e) {
		return "", false
	}
	if e.Str == nil {
		return "", false
	}
	return e.Str.Value, true
}

// Delete
func (s *inMemoryStore) Delete(key string) {
	delete(s.data, key)
}
