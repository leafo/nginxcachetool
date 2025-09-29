package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

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

	if err := executeList(ctx, root, *workers, filter, *jsonOut, *matchAll); err != nil {
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

func executeList(ctx context.Context, root string, workers int, filter func(*cacheEntry) bool, jsonOut bool, matchAll bool) error {
	results := scanCache(ctx, root, workers)

	var entries []*cacheEntry
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
		entries = append(entries, res.Entry)
	}

	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Metadata.Key == entries[j].Metadata.Key {
			return entries[i].Path < entries[j].Path
		}
		return entries[i].Metadata.Key < entries[j].Metadata.Key
	})

	if jsonOut {
		if err := writeJSONList(entries, root); err != nil {
			return err
		}
	} else {
		writePrettyList(entries, root)
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
	fmt.Printf("Total size: %s (%d bytes)\n", formatBytes(float64(totalSize)), totalSize)
	fmt.Printf("Average size: %s\n", formatBytes(avgSize))
	fmt.Printf("Min size: %s (%d bytes)\n", formatBytes(float64(minSize)), minSize)
	fmt.Printf("Max size: %s (%d bytes)\n", formatBytes(float64(maxSize)), maxSize)
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
		if _, err := io.Copy(os.Stdout, file); err != nil {
			file.Close()
			return fmt.Errorf("copy %s: %w", res.Entry.Path, err)
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

func writeJSONList(entries []*cacheEntry, root string) error {
	payload := make([]outputEntry, 0, len(entries))
	for _, e := range entries {
		rel, _ := filepath.Rel(root, e.Path)
		payload = append(payload, outputEntry{
			Key:           e.Metadata.Key,
			StatusLine:    e.Metadata.StatusLine,
			StatusCode:    e.Metadata.StatusCode,
			Path:          e.Path,
			RelativePath:  rel,
			SizeBytes:     e.Size,
			ContentLength: e.Metadata.ContentLength,
			StoredAt:      e.ModTime,
			Headers:       e.Metadata.Headers,
		})
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(payload)
}

func writePrettyList(entries []*cacheEntry, root string) {
	for _, e := range entries {
		rel, err := filepath.Rel(root, e.Path)
		if err != nil {
			rel = e.Path
		}
		fmt.Printf("KEY: %s\n", e.Metadata.Key)
		fmt.Printf("  Status: %s\n", e.Metadata.StatusLine)
		fmt.Printf("  Path: %s\n", rel)
		fmt.Printf("  Stored: %s\n", e.ModTime.Format(time.RFC3339))
		fmt.Printf("  Size: %s (%d bytes)", formatBytes(float64(e.Size)), e.Size)
		if e.Metadata.ContentLength > 0 {
			fmt.Printf(" (Content-Length: %s (%d))", formatBytes(float64(e.Metadata.ContentLength)), e.Metadata.ContentLength)
		}
		if ct, ok := e.Metadata.Headers["content-type"]; ok {
			fmt.Printf("\n  Content-Type: %s", ct)
		}
		fmt.Println()
		fmt.Println()
	}
	fmt.Printf("Matched %d entries\n", len(entries))
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
