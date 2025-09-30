package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/fsnotify/fsnotify"
)

const (
	colorReset  = "\x1b[0m"
	colorGreen  = "\x1b[32m"
	colorYellow = "\x1b[33m"
	colorRed    = "\x1b[31m"
)

var errSkipCacheFile = errors.New("skip cache file")

func main() {
	fs := flag.NewFlagSet("nginxcachetool", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [flags] <cache-dir>\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Flags:\n")
		fs.PrintDefaults()
	}

	workers := fs.Int("workers", 0, "Number of concurrent workers (default: number of CPUs)")
	jsonOut := fs.Bool("json", false, "Emit JSON instead of human-readable output")
	purge := fs.Bool("purge", false, "Delete matched cache entries")
	dryRun := fs.Bool("dry-run", false, "When purging, show what would be removed without deleting")
	matchAll := fs.Bool("all", false, "Match every cache entry (dangerous)")
	printFile := fs.Bool("print-file", false, "Write raw bytes of the first matched cache file to stdout")
	summary := fs.Bool("summary", false, "Print aggregate statistics for matched entries")
	watch := fs.Bool("watch", false, "Watch the cache directory and report file events")
	full := fs.Bool("full", false, "Show all metadata, including headers, for each entry")

	var contains stringSliceFlag
	fs.Var(&contains, "contains", "Substring to match against cache key, path, or content-type (repeatable)")

	var statusFilters stringSliceFlag
	fs.Var(&statusFilters, "status", "Filter by HTTP status code (repeatable)")

	if err := fs.Parse(os.Args[1:]); err != nil {
		if err == flag.ErrHelp {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	args := fs.Args()
	if len(args) != 1 {
		fs.Usage()
		os.Exit(1)
	}
	root := args[0]

	if *dryRun && !*purge {
		fmt.Fprintln(os.Stderr, "--dry-run can only be used together with --purge")
		os.Exit(1)
	}
	if *printFile && *purge {
		fmt.Fprintln(os.Stderr, "--print-file cannot be combined with --purge")
		os.Exit(1)
	}
	if *printFile && *jsonOut {
		fmt.Fprintln(os.Stderr, "--print-file cannot be combined with --json")
		os.Exit(1)
	}
	if *summary && *purge {
		fmt.Fprintln(os.Stderr, "--summary cannot be combined with --purge")
		os.Exit(1)
	}
	if *summary && *printFile {
		fmt.Fprintln(os.Stderr, "--summary cannot be combined with --print-file")
		os.Exit(1)
	}
	if *summary && *jsonOut {
		fmt.Fprintln(os.Stderr, "--summary cannot be combined with --json")
		os.Exit(1)
	}
	if *watch && *purge {
		fmt.Fprintln(os.Stderr, "--watch cannot be combined with --purge")
		os.Exit(1)
	}
	if *watch && *printFile {
		fmt.Fprintln(os.Stderr, "--watch cannot be combined with --print-file")
		os.Exit(1)
	}
	if *watch && *summary {
		fmt.Fprintln(os.Stderr, "--watch cannot be combined with --summary")
		os.Exit(1)
	}
	if *watch && *jsonOut {
		fmt.Fprintln(os.Stderr, "--watch cannot be combined with --json")
		os.Exit(1)
	}

	filter, err := buildEntryFilter(contains, statusFilters)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx := context.Background()

	if *purge {
		if !*matchAll && len(contains) == 0 && len(statusFilters) == 0 {
			fmt.Fprintln(os.Stderr, "refusing to purge without any filters; specify --contains, --status, or --all")
			os.Exit(1)
		}
		if err := executePurge(ctx, root, *workers, filter, *dryRun, *matchAll); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *watch {
		if err := executeWatch(root); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *summary {
		if err := executeSummary(ctx, root, *workers, filter, *matchAll); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *printFile {
		if err := executePrintFile(root, *workers, filter, *matchAll); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if err := executeList(ctx, root, *workers, filter, *jsonOut, *matchAll, *full); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type stringSliceFlag []string

func (s *stringSliceFlag) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSliceFlag) Set(value string) error {
	*s = append(*s, value)
	return nil
}

func executeList(ctx context.Context, root string, workers int, filter func(*cacheEntry) bool, jsonOut bool, matchAll bool, full bool) error {
	results := scanCache(ctx, root, workers)

	var encounteredErr error
	matched := 0

	var enc *json.Encoder
	if jsonOut {
		enc = json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
	}

	for res := range results {
		if res.Err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", res.Err)
			if encounteredErr == nil {
				encounteredErr = res.Err
			}
			continue
		}
		if res.Entry == nil {
			continue
		}
		if !matchAll && !filter(res.Entry) {
			continue
		}

		matched++

		if jsonOut {
			if err := writeJSONEntry(enc, res.Entry, root); err != nil {
				fmt.Fprintf(os.Stderr, "encode json %s: %v\n", res.Entry.Path, err)
				if encounteredErr == nil {
					encounteredErr = err
				}
			}
			continue
		}

		writePrettyEntry(res.Entry, root, full)
	}

	if !jsonOut {
		fmt.Printf("Matched %d entries\n", matched)
	}

	return encounteredErr
}

func executeSummary(ctx context.Context, root string, workers int, filter func(*cacheEntry) bool, matchAll bool) error {
	results := scanCache(ctx, root, workers)

	var encounteredErr error
	var totalFiles int64
	var totalSize int64
	minSize := int64(-1)
	var maxSize int64
	contentCounts := make(map[string]int)
	statusCounts := make(map[int]int)
	ageCounts := map[string]int{
		"<1h":   0,
		"<1d":   0,
		"<7d":   0,
		"<30d":  0,
		">=30d": 0,
	}
	now := time.Now()

	for res := range results {
		if res.Err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", res.Err)
			if encounteredErr == nil {
				encounteredErr = res.Err
			}
			continue
		}
		if res.Entry == nil {
			continue
		}
		if !matchAll && !filter(res.Entry) {
			continue
		}

		totalFiles++
		size := res.Entry.Size
		totalSize += size
		if minSize == -1 || size < minSize {
			minSize = size
		}
		if size > maxSize {
			maxSize = size
		}

		ct := res.Entry.Metadata.Headers["content-type"]
		if ct == "" {
			ct = "(unknown)"
		} else {
			ct = strings.ToLower(ct)
		}
		contentCounts[ct]++

		statusCounts[res.Entry.Metadata.StatusCode]++

		age := now.Sub(res.Entry.ModTime)
		switch {
		case age < time.Hour:
			ageCounts["<1h"]++
		case age < 24*time.Hour:
			ageCounts["<1d"]++
		case age < 7*24*time.Hour:
			ageCounts["<7d"]++
		case age < 30*24*time.Hour:
			ageCounts["<30d"]++
		default:
			ageCounts[">=30d"]++
		}
	}

	if totalFiles == 0 {
		fmt.Println("No matching cache entries found.")
		return encounteredErr
	}

	avgSize := float64(totalSize) / float64(totalFiles)

	fmt.Printf("Summary for %s\n", root)
	fmt.Printf("Total files: %d\n", totalFiles)
	fmt.Printf("Total size: %s\n", formatBytes(float64(totalSize)))
	fmt.Printf("Average size: %s\n", formatBytes(avgSize))
	fmt.Printf("Min size: %s\n", formatBytes(float64(minSize)))
	fmt.Printf("Max size: %s\n", formatBytes(float64(maxSize)))
	fmt.Println()

	fmt.Println("Content Types:")
	contentKeys := make([]string, 0, len(contentCounts))
	for k := range contentCounts {
		contentKeys = append(contentKeys, k)
	}
	sort.Strings(contentKeys)
	for _, k := range contentKeys {
		fmt.Printf("  %s: %d\n", k, contentCounts[k])
	}

	fmt.Println()
	fmt.Println("Status Codes:")
	statusKeys := make([]int, 0, len(statusCounts))
	for k := range statusCounts {
		statusKeys = append(statusKeys, k)
	}
	sort.Ints(statusKeys)
	for _, k := range statusKeys {
		fmt.Printf("  %d: %d\n", k, statusCounts[k])
	}

	fmt.Println()
	fmt.Println("Age Distribution:")
	fmt.Printf("  <1h: %d\n", ageCounts["<1h"])
	fmt.Printf("  <1d: %d\n", ageCounts["<1d"])
	fmt.Printf("  <7d: %d\n", ageCounts["<7d"])
	fmt.Printf("  <30d: %d\n", ageCounts["<30d"])
	fmt.Printf("  >=30d: %d\n", ageCounts[">=30d"])

	return encounteredErr
}

func executePurge(ctx context.Context, root string, workers int, filter func(*cacheEntry) bool, dryRun, matchAll bool) error {
	results := scanCache(ctx, root, workers)

	matched := 0
	deleted := 0
	var encounteredErr error

	for res := range results {
		if res.Err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", res.Err)
			if encounteredErr == nil {
				encounteredErr = res.Err
			}
			continue
		}
		if res.Entry == nil {
			continue
		}
		if !matchAll && !filter(res.Entry) {
			continue
		}

		matched++

		rel, err := filepath.Rel(root, res.Entry.Path)
		if err != nil {
			rel = res.Entry.Path
		}

		if dryRun {
			fmt.Printf("[dry-run] would remove %s (KEY: %s)\n", rel, res.Entry.Metadata.Key)
			continue
		}

		if err := os.Remove(res.Entry.Path); err != nil {
			fmt.Fprintf(os.Stderr, "remove %s: %v\n", res.Entry.Path, err)
			if encounteredErr == nil {
				encounteredErr = err
			}
			continue
		}

		fmt.Printf("Removed %s (KEY: %s)\n", rel, res.Entry.Metadata.Key)
		deleted++
	}

	fmt.Printf("Matched %d entries, removed %d\n", matched, deleted)

	return encounteredErr
}

func executeWatch(root string) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer watcher.Close()

	addDir := func(path string) error {
		if err := watcher.Add(path); err != nil {
			return fmt.Errorf("watch %s: %w", path, err)
		}
		return nil
	}

	if err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if err := addDir(path); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("walk directories: %w", err)
	}

	fmt.Printf("Watching %s (press Ctrl+C to stop)\n", root)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	for {
		select {
		case event, ok := <-watcher.Events:
			if !ok {
				return nil
			}
			if event.Name == "" {
				continue
			}

			if event.Op&fsnotify.Create != 0 {
				if info, err := os.Stat(event.Name); err == nil && info.IsDir() {
					if err := addDir(event.Name); err != nil {
						fmt.Fprintf(os.Stderr, "watch add %s: %v\n", event.Name, err)
					}
					continue
				}
			}

			action := classifyCacheEvent(event.Op, event.Name)
			if action == nil {
				continue
			}

			rel := event.Name
			if r, err := filepath.Rel(root, event.Name); err == nil {
				rel = r
			}
			printWatchLine(action, rel, event.Name)

		case err, ok := <-watcher.Errors:
			if !ok {
				return nil
			}
			if err != nil {
				fmt.Fprintf(os.Stderr, "watch error: %v\n", err)
			}
		case <-sigCh:
			fmt.Println("Stopping watch.")
			return nil
		}
	}
}

type watchAction struct {
	label        string
	color        string
	showMetadata bool
}

func printWatchLine(action *watchAction, relPath, fullPath string) {
	line := fmt.Sprintf("%s[%s]%s %s", action.color, action.label, colorReset, relPath)

	if action.showMetadata {
		entry, err := fetchCacheEntry(fullPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "watch metadata %s: %v\n", fullPath, err)
		} else {
			ct := entry.Metadata.Headers["content-type"]
			if ct == "" {
				ct = "(unknown)"
			}
			status := entry.Metadata.StatusCode

			var bodyBytes int64
			if entry.Metadata.ContentLength > 0 {
				bodyBytes = entry.Metadata.ContentLength
			} else {
				bodyBytes = entry.Size - int64(entry.Metadata.BodyStart)
				if bodyBytes < 0 {
					bodyBytes = entry.Size
				}
			}

			sizeLabel := formatBytes(float64(bodyBytes))
			line += fmt.Sprintf(" size=%s status=%d content-type=%s", sizeLabel, status, ct)
		}
	}

	fmt.Println(line)
}

func fetchCacheEntry(path string) (*cacheEntry, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	job := fileJob{path: path, size: info.Size(), modTime: info.ModTime()}
	return parseCacheFile(job)
}

func classifyCacheEvent(op fsnotify.Op, name string) *watchAction {
	if isTempCacheFile(name) {
		return nil
	}
	switch {
	case op&fsnotify.Create != 0:
		return &watchAction{label: "added", color: colorGreen, showMetadata: true}
	case op&fsnotify.Remove != 0:
		return &watchAction{label: "removed", color: colorRed}
	case op&fsnotify.Rename != 0:
		return &watchAction{label: "removed", color: colorRed}
	case op&fsnotify.Write != 0:
		return &watchAction{label: "updated", color: colorYellow}
	default:
		return nil
	}
}

func isTempCacheFile(name string) bool {
	base := filepath.Base(name)
	dot := strings.LastIndex(base, ".")
	if dot == -1 {
		return false
	}
	suffix := base[dot+1:]
	if len(suffix) != 10 {
		return false
	}
	for _, ch := range suffix {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}

func executePrintFile(root string, workers int, filter func(*cacheEntry) bool, matchAll bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results := scanCache(ctx, root, workers)

	var encounteredErr error
	printed := false

	for res := range results {
		if res.Err != nil {
			fmt.Fprintf(os.Stderr, "scan error: %v\n", res.Err)
			if encounteredErr == nil {
				encounteredErr = res.Err
			}
			continue
		}
		if res.Entry == nil {
			continue
		}
		if printed {
			continue
		}
		if !matchAll && !filter(res.Entry) {
			continue
		}

		cancel()

		file, err := os.Open(res.Entry.Path)
		if err != nil {
			return fmt.Errorf("open %s: %w", res.Entry.Path, err)
		}

		bodyStart := res.Entry.Metadata.BodyStart
		if bodyStart <= 0 {
			file.Close()
			return fmt.Errorf("cache file missing body offset %s", res.Entry.Path)
		}
		if int64(bodyStart) >= res.Entry.Size {
			file.Close()
			return fmt.Errorf("body offset %d beyond file size for %s", bodyStart, res.Entry.Path)
		}
		if _, err := file.Seek(int64(bodyStart), io.SeekStart); err != nil {
			file.Close()
			return fmt.Errorf("seek %s: %w", res.Entry.Path, err)
		}

		if res.Entry.Metadata.ContentLength > 0 {
			if _, err := io.CopyN(os.Stdout, file, res.Entry.Metadata.ContentLength); err != nil {
				file.Close()
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return fmt.Errorf("cached body truncated for %s: %w", res.Entry.Path, err)
				}
				return fmt.Errorf("copy %s: %w", res.Entry.Path, err)
			}
		} else {
			if _, err := io.Copy(os.Stdout, file); err != nil {
				file.Close()
				return fmt.Errorf("copy %s: %w", res.Entry.Path, err)
			}
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("close %s: %w", res.Entry.Path, err)
		}
		printed = true
	}

	if printed {
		return encounteredErr
	}
	if encounteredErr != nil {
		return encounteredErr
	}
	return fmt.Errorf("no matching cache entries found")
}

// Remaining helpers unchanged below

type outputEntry struct {
	Key           string            `json:"key"`
	StatusLine    string            `json:"status_line"`
	StatusCode    int               `json:"status_code"`
	Path          string            `json:"path"`
	RelativePath  string            `json:"relative_path"`
	SizeBytes     int64             `json:"size_bytes"`
	ContentLength int64             `json:"content_length"`
	StoredAt      time.Time         `json:"stored_at"`
	Headers       map[string]string `json:"headers"`
}

func writeJSONEntry(enc *json.Encoder, entry *cacheEntry, root string) error {
	rel, err := filepath.Rel(root, entry.Path)
	if err != nil {
		rel = entry.Path
	}

	return enc.Encode(outputEntry{
		Key:           entry.Metadata.Key,
		StatusLine:    entry.Metadata.StatusLine,
		StatusCode:    entry.Metadata.StatusCode,
		Path:          entry.Path,
		RelativePath:  rel,
		SizeBytes:     entry.Size,
		ContentLength: entry.Metadata.ContentLength,
		StoredAt:      entry.ModTime,
		Headers:       entry.Metadata.Headers,
	})
}

func writePrettyEntry(entry *cacheEntry, root string, full bool) {
	rel, err := filepath.Rel(root, entry.Path)
	if err != nil {
		rel = entry.Path
	}

	fmt.Printf("KEY: %s\n", entry.Metadata.Key)
	fmt.Printf("  Status: %s\n", entry.Metadata.StatusLine)
	fmt.Printf("  Path: %s\n", rel)
	fmt.Printf("  Stored: %s\n", entry.ModTime.Format(time.RFC3339))
	fmt.Printf("  Size: %s", formatBytes(float64(entry.Size)))
	if entry.Metadata.ContentLength > 0 {
		fmt.Printf(" (Content-Length: %s)", formatBytes(float64(entry.Metadata.ContentLength)))
	}
	fmt.Println()

	if !full {
		if ct, ok := entry.Metadata.Headers["content-type"]; ok {
			fmt.Printf("  Content-Type: %s\n", ct)
		}
		fmt.Println()
		return
	}

	fmt.Printf("  Status Code: %d\n", entry.Metadata.StatusCode)
	fmt.Printf("  Version: %d\n", entry.Metadata.Version)
	fmt.Printf("  Header Offset: %d\n", entry.Metadata.HeaderStart)
	fmt.Printf("  Body Offset: %d\n", entry.Metadata.BodyStart)
	if entry.Metadata.ContentLength > 0 {
		fmt.Printf("  Content-Length: %d bytes\n", entry.Metadata.ContentLength)
	}
	fmt.Printf("  Raw Header Bytes: %d\n", len(entry.Metadata.RawHeaderBlock))
	headers := make([]string, 0, len(entry.Metadata.Headers))
	for k := range entry.Metadata.Headers {
		headers = append(headers, k)
	}
	sort.Strings(headers)
	fmt.Println("  Headers:")
	for _, k := range headers {
		fmt.Printf("    %s: %s\n", k, entry.Metadata.Headers[k])
	}
	fmt.Println()
	fmt.Println("  Raw Header Block:")
	for _, line := range strings.Split(entry.Metadata.RawHeaderBlock, "\n") {
		if line == "" {
			fmt.Println("    ")
			continue
		}
		fmt.Printf("    %s\n", line)
	}
	fmt.Println()
	fmt.Println()
}

func buildEntryFilter(contains, statuses []string) (func(*cacheEntry) bool, error) {
	normalized := make([]string, 0, len(contains))
	for _, c := range contains {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		normalized = append(normalized, strings.ToLower(c))
	}

	statusSet := make(map[int]struct{})
	for _, s := range statuses {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		code, err := strconv.Atoi(s)
		if err != nil {
			return nil, fmt.Errorf("invalid status code %q", s)
		}
		statusSet[code] = struct{}{}
	}

	return func(e *cacheEntry) bool {
		if len(normalized) > 0 {
			keyLower := strings.ToLower(e.Metadata.Key)
			pathLower := strings.ToLower(e.Path)
			matched := false
			for _, sub := range normalized {
				if strings.Contains(keyLower, sub) || strings.Contains(pathLower, sub) {
					matched = true
					break
				}
				if ct, ok := e.Metadata.Headers["content-type"]; ok {
					if strings.Contains(strings.ToLower(ct), sub) {
						matched = true
						break
					}
				}
			}
			if !matched {
				return false
			}
		}

		if len(statusSet) > 0 {
			if _, ok := statusSet[e.Metadata.StatusCode]; !ok {
				return false
			}
		}

		return true
	}, nil
}

func formatBytes(v float64) string {
	if v < 0 {
		return "-" + formatBytes(-v)
	}
	units := []string{"B", "KB", "MB", "GB", "TB", "PB"}
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 {
		return fmt.Sprintf("%.0f %s", v, units[i])
	}
	return fmt.Sprintf("%.2f %s", v, units[i])
}
