package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type watchMetadataStore struct {
	mu      sync.RWMutex
	entries map[string]*cacheEntry
}

func newWatchMetadataStore() *watchMetadataStore {
	return &watchMetadataStore{
		entries: make(map[string]*cacheEntry),
	}
}

func (s *watchMetadataStore) Load(ctx context.Context, root string, workers int) error {
	if ctx == nil {
		ctx = context.Background()
	}

	results := scanCache(ctx, root, workers)
	var firstErr error

	for res := range results {
		if res.Err != nil {
			if errors.Is(res.Err, context.Canceled) {
				continue
			}
			if firstErr == nil {
				firstErr = res.Err
			}
			fmt.Fprintf(os.Stderr, "metadata preload error: %v\n", res.Err)
			continue
		}
		if res.Entry == nil {
			continue
		}
		s.Set(res.Entry)
	}

	return firstErr
}

func (s *watchMetadataStore) Set(entry *cacheEntry) {
	if entry == nil {
		return
	}
	key := filepath.Clean(entry.Path)

	s.mu.Lock()
	s.entries[key] = entry
	s.mu.Unlock()
}

func (s *watchMetadataStore) Remove(path string) *cacheEntry {
	key := filepath.Clean(path)

	s.mu.Lock()
	entry := s.entries[key]
	if entry != nil {
		delete(s.entries, key)
	}
	s.mu.Unlock()

	return entry
}

func (s *watchMetadataStore) Get(path string) (*cacheEntry, bool) {
	key := filepath.Clean(path)

	s.mu.RLock()
	entry, ok := s.entries[key]
	s.mu.RUnlock()

	return entry, ok
}

func (s *watchMetadataStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}
