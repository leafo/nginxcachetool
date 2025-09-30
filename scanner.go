package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	metadataHeaderPrefix = 0x3c
	headerStartOffset    = 0x36
	bodyStartOffset      = 0x38
	maxMetadataBytes     = 512 * 1024
)

type cacheEntry struct {
	Path     string
	Size     int64
	ModTime  time.Time
	Metadata cacheMetadata
}

type cacheMetadata struct {
	Version        byte
	HeaderStart    int
	BodyStart      int
	Key            string
	StatusLine     string
	StatusCode     int
	Headers        map[string]string
	ContentLength  int64
	RawHeaderBlock string
}

type cacheResult struct {
	Entry *cacheEntry
	Err   error
}

type fileJob struct {
	path    string
	size    int64
	modTime time.Time
}

func defaultWorkerCount() int {
	if n := runtime.NumCPU(); n > 0 {
		return n
	}
	return 4
}

func scanCache(ctx context.Context, root string, workers int) <-chan cacheResult {
	if ctx == nil {
		ctx = context.Background()
	}

	if workers <= 0 {
		workers = defaultWorkerCount()
	}

	results := make(chan cacheResult)
	jobs := make(chan fileJob, workers*2)

	go func() {
		defer close(results)

		var wg sync.WaitGroup
		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case job, ok := <-jobs:
						if !ok {
							return
						}
						entry, err := parseCacheFile(job)
						if err != nil {
							if errors.Is(err, errSkipCacheFile) {
								continue
							}
							select {
							case <-ctx.Done():
								return
							case results <- cacheResult{Err: err}:
							}
							continue
						}
						select {
						case <-ctx.Done():
							return
						case results <- cacheResult{Entry: entry}:
						}
					}
				}
			}()
		}

		walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				select {
				case <-ctx.Done():
				case results <- cacheResult{Err: fmt.Errorf("walk %s: %w", path, err)}:
				}
				return err
			}

			select {
			case <-ctx.Done():
				return context.Canceled
			default:
			}

			if d.IsDir() || !d.Type().IsRegular() {
				return nil
			}

			info, infoErr := d.Info()
			if infoErr != nil {
				select {
				case <-ctx.Done():
				case results <- cacheResult{Err: fmt.Errorf("stat %s: %w", path, infoErr)}:
				}
				return nil
			}

			job := fileJob{path: path, size: info.Size(), modTime: info.ModTime()}

			select {
			case <-ctx.Done():
				return context.Canceled
			case jobs <- job:
			}

			return nil
		})

		close(jobs)
		wg.Wait()

		if walkErr != nil && !errors.Is(walkErr, context.Canceled) {
			select {
			case <-ctx.Done():
			case results <- cacheResult{Err: walkErr}:
			}
		}
	}()

	return results
}

func parseCacheFile(job fileJob) (*cacheEntry, error) {
	if isTempCacheFile(job.path) {
		return nil, errSkipCacheFile
	}

	f, err := os.Open(job.path)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", job.path, err)
	}
	defer f.Close()

	header := make([]byte, metadataHeaderPrefix)
	if _, err := io.ReadFull(f, header); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errSkipCacheFile
		}
		return nil, fmt.Errorf("read header %s: %w", job.path, err)
	}

	if len(header) < bodyStartOffset+4 {
		return nil, errSkipCacheFile
	}

	version := header[0]
	headerStart := int(binary.LittleEndian.Uint16(header[headerStartOffset : headerStartOffset+2]))
	bodyStart := int(binary.LittleEndian.Uint32(header[bodyStartOffset : bodyStartOffset+4]))

	if bodyStart <= 0 || bodyStart > maxMetadataBytes {
		return nil, fmt.Errorf("metadata size %d out of bounds in %s", bodyStart, job.path)
	}

	if headerStart <= 0 || headerStart > bodyStart {
		return nil, fmt.Errorf("invalid header/body start (%d/%d) in %s", headerStart, bodyStart, job.path)
	}

	metadataBuf := make([]byte, bodyStart)
	section := io.NewSectionReader(f, 0, int64(bodyStart))
	if _, err := io.ReadFull(section, metadataBuf); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errSkipCacheFile
		}
		return nil, fmt.Errorf("read metadata %s: %w", job.path, err)
	}

	md, err := parseMetadata(metadataBuf, headerStart, bodyStart)
	if err != nil {
		return nil, fmt.Errorf("parse metadata %s: %w", job.path, err)
	}
	md.Version = version

	return &cacheEntry{
		Path:     job.path,
		Size:     job.size,
		ModTime:  job.modTime,
		Metadata: md,
	}, nil
}

func parseMetadata(buf []byte, headerStart, bodyStart int) (cacheMetadata, error) {
	md := cacheMetadata{
		HeaderStart: headerStart,
		BodyStart:   bodyStart,
		Headers:     make(map[string]string),
	}

	keyIdx := bytes.Index(buf, []byte("KEY:"))
	if keyIdx >= 0 {
		lineEnd := bytes.IndexByte(buf[keyIdx:], '\n')
		if lineEnd == -1 {
			lineEnd = len(buf) - keyIdx
		}
		keyLine := string(buf[keyIdx : keyIdx+lineEnd])
		parts := strings.SplitN(keyLine, ":", 2)
		if len(parts) == 2 {
			md.Key = strings.TrimSpace(parts[1])
		}
	}

	if headerStart > len(buf) {
		return md, fmt.Errorf("header start %d beyond metadata buffer", headerStart)
	}
	if bodyStart > len(buf) {
		return md, fmt.Errorf("body start %d beyond metadata buffer", bodyStart)
	}

	headerBlock := buf[headerStart:bodyStart]
	if len(headerBlock) == 0 {
		return md, errors.New("empty HTTP header block")
	}

	headerText := normalizeNewlines(string(headerBlock))
	md.RawHeaderBlock = headerText

	scanner := bufio.NewScanner(strings.NewReader(headerText))
	scanner.Split(bufio.ScanLines)

	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return md, fmt.Errorf("scan status line: %w", err)
		}
		return md, errors.New("missing HTTP status line")
	}

	statusLine := strings.TrimRight(scanner.Text(), "\r")
	md.StatusLine = statusLine
	md.StatusCode = parseStatusCode(statusLine)

	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if line == "" {
			break
		}
		if !strings.Contains(line, ":") {
			continue
		}
		parts := strings.SplitN(line, ":", 2)
		name := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		if name == "" {
			continue
		}
		md.Headers[strings.ToLower(name)] = value
	}

	if err := scanner.Err(); err != nil {
		return md, fmt.Errorf("scan headers: %w", err)
	}

	if cl, ok := md.Headers["content-length"]; ok {
		if v, err := strconv.ParseInt(cl, 10, 64); err == nil {
			md.ContentLength = v
		}
	}

	return md, nil
}

func normalizeNewlines(s string) string {
	return strings.ReplaceAll(s, "\r\n", "\n")
}

func parseStatusCode(statusLine string) int {
	fields := strings.Fields(statusLine)
	if len(fields) < 2 {
		return 0
	}
	code, err := strconv.Atoi(fields[1])
	if err != nil {
		return 0
	}
	return code
}
