package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseCacheFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	cachePath := filepath.Join(tmpDir, "cache-entry")

	const version byte = 5
	keyLine := []byte("KEY: sample-cache-key\n")
	headerBlock := []byte("HTTP/1.1 200 OK\r\n" +
		"Content-Type: text/html; charset=utf-8\r\n" +
		"Content-Length: 123\r\n" +
		"X-Custom: value\r\n" +
		"\r\n")

	headerStart := metadataHeaderPrefix + len(keyLine)
	bodyStart := headerStart + len(headerBlock)

	header := make([]byte, metadataHeaderPrefix)
	header[0] = version
	binary.LittleEndian.PutUint16(header[headerStartOffset:], uint16(headerStart))
	binary.LittleEndian.PutUint16(header[bodyStartOffset:], uint16(bodyStart))

	metadata := make([]byte, bodyStart)
	copy(metadata, header)
	copy(metadata[metadataHeaderPrefix:], keyLine)
	copy(metadata[headerStart:], headerBlock)

	fileData := append(metadata, []byte("response-body")...)

	if err := os.WriteFile(cachePath, fileData, 0o644); err != nil {
		t.Fatalf("write cache file: %v", err)
	}

	info, err := os.Stat(cachePath)
	if err != nil {
		t.Fatalf("stat cache file: %v", err)
	}

	modTime := time.Unix(1700000000, 0)
	job := fileJob{path: cachePath, size: info.Size(), modTime: modTime}

	entry, err := parseCacheFile(job)
	if err != nil {
		t.Fatalf("parse cache file: %v", err)
	}

	if entry.Path != cachePath {
		t.Fatalf("path mismatch: got %q want %q", entry.Path, cachePath)
	}

	if entry.Size != info.Size() {
		t.Fatalf("size mismatch: got %d want %d", entry.Size, info.Size())
	}

	if !entry.ModTime.Equal(modTime) {
		t.Fatalf("mod time mismatch: got %v want %v", entry.ModTime, modTime)
	}

	md := entry.Metadata
	if md.Version != version {
		t.Fatalf("version mismatch: got %d want %d", md.Version, version)
	}

	if md.HeaderStart != headerStart {
		t.Fatalf("header start mismatch: got %d want %d", md.HeaderStart, headerStart)
	}

	if md.BodyStart != bodyStart {
		t.Fatalf("body start mismatch: got %d want %d", md.BodyStart, bodyStart)
	}

	if md.Key != "sample-cache-key" {
		t.Fatalf("key mismatch: got %q want %q", md.Key, "sample-cache-key")
	}

	if md.StatusLine != "HTTP/1.1 200 OK" {
		t.Fatalf("status line mismatch: got %q", md.StatusLine)
	}

	if md.StatusCode != 200 {
		t.Fatalf("status code mismatch: got %d want %d", md.StatusCode, 200)
	}

	if got := md.Headers["content-type"]; got != "text/html; charset=utf-8" {
		t.Fatalf("content-type mismatch: got %q", got)
	}

	if got := md.Headers["content-length"]; got != "123" {
		t.Fatalf("content-length header mismatch: got %q", got)
	}

	if got := md.Headers["x-custom"]; got != "value" {
		t.Fatalf("x-custom header mismatch: got %q", got)
	}

	if md.ContentLength != 123 {
		t.Fatalf("content length mismatch: got %d want %d", md.ContentLength, 123)
	}

	expectedRaw := normalizeNewlines(string(headerBlock))
	if md.RawHeaderBlock != expectedRaw {
		t.Fatalf("raw header block mismatch:\n got: %q\nwant: %q", md.RawHeaderBlock, expectedRaw)
	}
}
