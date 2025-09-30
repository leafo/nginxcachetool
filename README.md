# nginxcachetool

A command line tool to work with the proxy cache directory created by NGINX's
`proxy_cache` and `proxy_cache_path` directive. This tool can help you
understand what's in your cache (`--summary`), search for items (`--contains`),
purge items (`--purge`), and watch the changes to the cache in real time
(`--watch`).

NGINX proxy caching stores each cached response as a flat file whose directory
path is derived from the MD5 hash of the cache key. The file begins with a
fixed binary header containing cache metadata (version, offsets, timing),
followed by an ASCII block that embeds the original response headers and the
cached body. This program will recursively scan a directory for these cache
files and only read the metadata from each file to understand what it contains.

Enable proxy caching in NGINX with `proxy_cache_path` and `proxy_cache`:

<details>
<summary>Enable proxy caching in NGINX with `proxy_cache_path` and `proxy_cache`:</summary>

```nginx
proxy_cache_path /var/cache/nginx levels=1:2 keys_zone=my_cache:10m inactive=60m;

server {
    location / {
        proxy_cache my_cache;
        proxy_pass http://upstream_backend;
    }
}
```
</details>

## Installing

Install it into your Go bin directory (usually `~/go/bin`) with:

```sh
go install github.com/leafo/nginxcachetool
```

## Building

In the checked out

```sh
go build
```

This produces the `nginxcachetool` binary in the project root.

## Usage

```
nginxcachetool [flags] <cache-dir>
```

Common flags:

- `--contains <needle>` (repeatable) filters by substring on the cache entry’s
  key (`KEY:` value), full on-disk file path, or the `Content-Type` response
  header.
- `--status <code>` (repeatable) filters by cached HTTP status codes.
- `--json` streams matching entries as newline-delimited JSON.
- `--full` prints every metadata field and header when using human-readable
  output.
- `--limit <n>` stops after processing N matched entries (default: unlimited).
- `--summary` aggregates totals, sizes, and age buckets instead of listing.
- `--print-file` writes the cached response body (first match) to stdout.
- `--purge` deletes matching entries; add `--dry-run` to preview removals.
- `--all` disables filtering (required for dangerous operations like
  unconditional purge).
- `--workers <n>` sets the number of concurrent metadata scanners (defaults to
  CPU count).

### Examples

List HTML cache entries and show their headers:

```sh
nginxcachetool --contains text/html --full /var/cache/nginx
```

Sample output:

```
KEY: production:5::/game-assets/tag-8-bit/tag-pirates:b1:
  Status: HTTP/1.1 200 OK
  Path: 0/00/0514439d879523f9d120d519b6cb7000
  Stored: 2025-07-29T22:20:41-07:00
  Size: 38.83 KB
  Status Code: 200
  Version: 5
  Header Offset: 395
  Body Offset: 705
  Raw Header Bytes: 298
  Raw Header Block:
    HTTP/1.1 200 OK
    Date: Wed, 30 Jul 2025 05:20:41 GMT
    Content-Type: text/html
    Transfer-Encoding: chunked
    Connection: keep-alive
    X-Accel-Expires: 600
    Server: lapis
    X-Frame-Options: SAMEORIGIN
    X-XSS-Protection: 1; mode=block
    X-Content-Type-Options: nosniff
    Referrer-Policy: no-referrer-when-downgrade
```

Export JSON object containing metadata for for 200 responses to a file:

```sh
nginxcachetool --status 200 --json /var/cache/nginx > cache-200.jsonl
```

Preview purging every cached 404 response without deleting anything yet:

```sh
nginxcachetool --status 404 --purge --dry-run /var/cache/nginx
```

### Summary Mode

Gather high-level information about a subset of the cache with `--summary`.
This scans the matching entries once and prints totals, average size, minimum
and maximum sizes, content-type breakdowns, status counts, and age buckets
(`<1h`, `<1d`, `<7d`, `<30d`, `>=30d`).

Summary mode can optionally be combined with filters like `--contains` and
`--status`.

```sh
nginxcachetool --summary /var/cache/nginx
```

Sample output:

```
Summary for /var/cache/nginx
Total files: 661378
Total size: 13.56 GB
Average size: 21.50 KB
Min size: 625 B
Max size: 3.96 MB

Content Types:
  application/json: 105512
  application/rss+xml: 13
  application/xml: 316
  text/html: 555537

Status Codes:
  200: 550662
  301: 2178
  302: 108351
  404: 55
  451: 132
```

### Watch Mode

Use `--watch` to follow cache activity as it happens. The tool recurses through
the cache tree, subscribes to filesystem events with `fsnotify`, and reports
create/update/remove actions with color-coded lines that include the size,
status, and content type when available.

```sh
nginxcachetool --watch /var/cache/nginx
```

Press `Ctrl+C` to stop watching.
