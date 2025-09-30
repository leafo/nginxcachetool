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
	withMetadata := fs.Bool("with-metadata", false, "In watch mode, preload and track cache metadata for change reporting")
	full := fs.Bool("full", false, "Show all metadata, including headers, for each entry")
	limit := fs.Int("limit", 0, "Maximum number of entries to process (0 = unlimited)")

	var contains stringSliceFlag
	fs.Var(&contains, "contains", "Substring to match against cache key, path, or content-type (repeatable)")

	var statusFilters stringSliceFlag
	fs.Var(&statusFilters, "status", "Filter by HTTP status code (repeatable)")

	var contentTypeFilters stringSliceFlag
	fs.Var(&contentTypeFilters, "content-type", "Filter by response Content-Type header (repeatable)")

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
	if *limit < 0 {
		fmt.Fprintln(os.Stderr, "--limit cannot be negative")
		os.Exit(1)
	}
	if *watch && *limit > 0 {
		fmt.Fprintln(os.Stderr, "--watch cannot be combined with --limit")
		os.Exit(1)
	}
	if *withMetadata && !*watch {
		fmt.Fprintln(os.Stderr, "--with-metadata requires --watch")
		os.Exit(1)
	}

	filter, err := buildEntryFilter(contains, statusFilters, contentTypeFilters)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	ctx := context.Background()

	if *watch {
		if err := executeWatch(root, *withMetadata, *workers); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *purge {
		if !*matchAll && len(contains) == 0 && len(statusFilters) == 0 && len(contentTypeFilters) == 0 {
			fmt.Fprintln(os.Stderr, "refusing to purge without any filters; specify --contains, --status, --content-type, or --all")
			os.Exit(1)
		}
		it := newEntryIterator(ctx, root, *workers, *matchAll, *limit, filter)
		if err := executePurge(it, root, *dryRun); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *summary {
		it := newEntryIterator(ctx, root, *workers, *matchAll, *limit, filter)
		if err := executeSummary(it, root); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	if *printFile {
		it := newEntryIterator(ctx, root, *workers, *matchAll, *limit, filter)
		if err := executePrintFile(it); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}

	it := newEntryIterator(ctx, root, *workers, *matchAll, *limit, filter)
	if err := executeList(it, root, *jsonOut, *full); err != nil {
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

func executeList(it *entryIterator, root string, jsonOut bool, full bool) error {
	defer it.Close()

	var encounteredErr error
	matched := 0

	var enc *json.Encoder
	if jsonOut {
		enc = json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
	}

	for entry := range it.Entries() {
		matched++

		if jsonOut {
			if err := writeJSONEntry(enc, entry, root); err != nil {
				fmt.Fprintf(os.Stderr, "encode json %s: %v\n", entry.Path, err)
				if encounteredErr == nil {
					encounteredErr = err
				}
			}
		} else {
			writePrettyEntry(entry, root, full)
		}
	}

	if !jsonOut {
		fmt.Printf("Matched %d entries\n", matched)
	}

	if encounteredErr != nil {
		return encounteredErr
	}
	return it.Err()
}

func executeSummary(it *entryIterator, root string) error {
	defer it.Close()

	var totalFiles int64
	var totalSize int64
	minSize := int64(-1)
	var maxSize int64
	var minPath string
	var maxPath string
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

	for entry := range it.Entries() {
		totalFiles++
		size := entry.Size
		totalSize += size
		if minSize == -1 || size < minSize {
			minSize = size
			rel := entry.Path
			if r, err := filepath.Rel(root, entry.Path); err == nil {
				rel = r
			}
			minPath = rel
		}
		if size > maxSize {
			maxSize = size
			rel := entry.Path
			if r, err := filepath.Rel(root, entry.Path); err == nil {
				rel = r
			}
			maxPath = rel
		}

		ct := entry.Metadata.Headers["content-type"]
		if ct == "" {
			ct = "(unknown)"
		} else {
			ct = strings.ToLower(ct)
		}
		contentCounts[ct]++

		statusCounts[entry.Metadata.StatusCode]++

		age := now.Sub(entry.ModTime)
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
		return it.Err()
	}

	avgSize := float64(totalSize) / float64(totalFiles)

	fmt.Printf("Summary for %s\n", root)
	fmt.Printf("Total files: %d\n", totalFiles)
	fmt.Printf("Total size: %s\n", formatBytes(float64(totalSize)))
	fmt.Printf("Average size: %s\n", formatBytes(avgSize))
	fmt.Printf("Min size: %s (%s)\n", formatBytes(float64(minSize)), minPath)
	fmt.Printf("Max size: %s (%s)\n", formatBytes(float64(maxSize)), maxPath)
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

	return it.Err()
}

func executePurge(it *entryIterator, root string, dryRun bool) error {
	defer it.Close()

	matched := 0
	deleted := 0
	var encounteredErr error

	for entry := range it.Entries() {
		matched++

		rel, err := filepath.Rel(root, entry.Path)
		if err != nil {
			rel = entry.Path
		}

		if dryRun {
			fmt.Printf("[dry-run] would remove %s (KEY: %s)\n", rel, entry.Metadata.Key)
		} else {
			if err := os.Remove(entry.Path); err != nil {
				fmt.Fprintf(os.Stderr, "remove %s: %v\n", entry.Path, err)
				if encounteredErr == nil {
					encounteredErr = err
				}
			} else {
				fmt.Printf("Removed %s (KEY: %s)\n", rel, entry.Metadata.Key)
				deleted++
			}
		}
	}

	fmt.Printf("Matched %d entries, removed %d\n", matched, deleted)

	if encounteredErr != nil {
		return encounteredErr
	}
	return it.Err()
}

func executeWatch(root string, withMetadata bool, workers int) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("create watcher: %w", err)
	}
	defer watcher.Close()

	var store *watchMetadataStore
	if withMetadata {
		fmt.Printf("Preloading cache metadata from %s...\n", root)
		store = newWatchMetadataStore()
		if err := store.Load(context.Background(), root, workers); err != nil {
			fmt.Fprintf(os.Stderr, "initial metadata load: %v\n", err)
		}
		fmt.Printf("Loaded metadata for %d cache entries.\n", store.Count())
	}

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

			var cachedEntry *cacheEntry
			if store != nil {
				switch action.label {
				case "added", "updated":
					entry, err := fetchCacheEntry(event.Name)
					if err != nil {
						fmt.Fprintf(os.Stderr, "watch metadata %s: %v\n", event.Name, err)
					} else {
						store.Set(entry)
						cachedEntry = entry
					}
				case "removed":
					cachedEntry = store.Remove(event.Name)
				}
			}

			printWatchLine(action, rel, event.Name, cachedEntry, store == nil)

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

func printWatchLine(action *watchAction, relPath, fullPath string, existing *cacheEntry, allowFetch bool) {
	line := fmt.Sprintf("%s[%s]%s %s", action.color, action.label, colorReset, relPath)

	entry := existing
	if entry == nil && allowFetch && action.showMetadata {
		var err error
		entry, err = fetchCacheEntry(fullPath)
		if err != nil {
			if !(action.label == "removed" && errors.Is(err, os.ErrNotExist)) {
				fmt.Fprintf(os.Stderr, "watch metadata %s: %v\n", fullPath, err)
			}
		}
	}

	if entry != nil {
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
		details := []string{
			fmt.Sprintf("size=%s", sizeLabel),
			fmt.Sprintf("status=%d", status),
			fmt.Sprintf("content-type=%s", ct),
		}

		if entry.Metadata.Key != "" {
			details = append(details, fmt.Sprintf("key=%s", entry.Metadata.Key))
		}

		line += " " + strings.Join(details, " ")
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
		return &watchAction{label: "removed", color: colorRed, showMetadata: true}
	case op&fsnotify.Rename != 0:
		return &watchAction{label: "removed", color: colorRed, showMetadata: true}
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

func executePrintFile(it *entryIterator) error {
	defer it.Close()

	printed := false

	for entry := range it.Entries() {
		if printed {
			break
		}

		it.Close()

		file, err := os.Open(entry.Path)
		if err != nil {
			return fmt.Errorf("open %s: %w", entry.Path, err)
		}

		bodyStart := entry.Metadata.BodyStart
		if bodyStart <= 0 {
			file.Close()
			return fmt.Errorf("cache file missing body offset %s", entry.Path)
		}
		if int64(bodyStart) >= entry.Size {
			file.Close()
			return fmt.Errorf("body offset %d beyond file size for %s", bodyStart, entry.Path)
		}
		if _, err := file.Seek(int64(bodyStart), io.SeekStart); err != nil {
			file.Close()
			return fmt.Errorf("seek %s: %w", entry.Path, err)
		}

		if entry.Metadata.ContentLength > 0 {
			if _, err := io.CopyN(os.Stdout, file, entry.Metadata.ContentLength); err != nil {
				file.Close()
				if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
					return fmt.Errorf("cached body truncated for %s: %w", entry.Path, err)
				}
				return fmt.Errorf("copy %s: %w", entry.Path, err)
			}
		} else {
			if _, err := io.Copy(os.Stdout, file); err != nil {
				file.Close()
				return fmt.Errorf("copy %s: %w", entry.Path, err)
			}
		}

		if err := file.Close(); err != nil {
			return fmt.Errorf("close %s: %w", entry.Path, err)
		}
		printed = true
		break
	}

	if printed {
		return it.Err()
	}

	if err := it.Err(); err != nil {
		return err
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
	fmt.Println("  Raw Header Block:")
	for _, line := range strings.Split(entry.Metadata.RawHeaderBlock, "\n") {
		if line == "" {
			fmt.Println("    ")
			continue
		}
		fmt.Printf("    %s\n", line)
	}
	fmt.Println()
}

func buildEntryFilter(contains, statuses, contentTypes []string) (func(*cacheEntry) bool, error) {
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

	contentMatchers := make([]string, 0, len(contentTypes))
	for _, ct := range contentTypes {
		ct = strings.TrimSpace(ct)
		if ct == "" {
			continue
		}
		contentMatchers = append(contentMatchers, strings.ToLower(ct))
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

		if len(contentMatchers) > 0 {
			ct := strings.ToLower(e.Metadata.Headers["content-type"])
			if ct == "" {
				return false
			}
			base := ct
			if idx := strings.Index(base, ";"); idx != -1 {
				base = strings.TrimSpace(base[:idx])
			}
			matched := false
			for _, needle := range contentMatchers {
				if base == needle || strings.Contains(ct, needle) {
					matched = true
					break
				}
			}
			if !matched {
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
