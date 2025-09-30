package main

import (
	"context"
	"fmt"
	"os"
	"sync"
)

type entryIterator struct {
	entries chan *cacheEntry
	cancel  context.CancelFunc
	errMu   sync.Mutex
	err     error
}

func newEntryIterator(ctx context.Context, root string, workers int, matchAll bool, limit int, filter func(*cacheEntry) bool) *entryIterator {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithCancel(ctx)

	out := make(chan *cacheEntry)
	it := &entryIterator{
		entries: out,
		cancel:  cancel,
	}

	go func() {
		defer close(out)
		defer cancel()

		results := scanCache(ctx, root, workers)
		matched := 0

		for res := range results {
			if res.Err != nil {
				fmt.Fprintf(os.Stderr, "scan error: %v\n", res.Err)
				it.recordErr(res.Err)
				continue
			}
			if res.Entry == nil {
				continue
			}
			if !matchAll && !filter(res.Entry) {
				continue
			}

			matched++

			select {
			case <-ctx.Done():
				return
			case out <- res.Entry:
			}

			if limit > 0 && matched >= limit {
				cancel()
				return
			}
		}
	}()

	return it
}

func (it *entryIterator) Entries() <-chan *cacheEntry {
	return it.entries
}

func (it *entryIterator) Close() {
	if it.cancel != nil {
		it.cancel()
	}
}

func (it *entryIterator) Err() error {
	it.errMu.Lock()
	defer it.errMu.Unlock()
	return it.err
}

func (it *entryIterator) recordErr(err error) {
	it.errMu.Lock()
	if it.err == nil {
		it.err = err
	}
	it.errMu.Unlock()
}
