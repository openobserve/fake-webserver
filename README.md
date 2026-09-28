# fake-webserver

It's a Fake Webserver to generate metrics data for Prometheus load testing.

## Features

- Generates configurable number of API endpoints
- Creates multiple label combinations (regions, versions, virtual nodes) for high cardinality testing
- Simulates multiple nodes from a single process via `-num-nodes`
- Simulates realistic traffic patterns with oscillating request rates
- Includes error simulation and periodic outages
- Exports Prometheus metrics at `/metrics` endpoint

## Usage

### Command-line flags

```bash
./fake-webserver [flags]

Flags:
  -num-endpoints int
        Number of API endpoints to generate (default 50)
  -num-regions int
        Number of region labels to generate (default 5)
  -num-versions int
        Number of version labels to generate (default 3)
  -num-nodes int
        Number of node labels to generate (default 1).
        1 uses this host's node name; >1 simulates names based on it
        (e.g. hostname-1, hostname-2, ...)
  -oscillation-period duration
        Duration of rate oscillation period (default 5m)
  -enable-process-metrics
        Include process_* metrics (default true)
  -enable-go-metrics
        Include go_* metrics (default true)
  -allow-metrics-compression
        Allow gzip compression of metrics (default true)
```

### Examples

**Default settings** (~4,860 time series):
```bash
./fake-webserver
```

**High cardinality test** (~31,200 time series):
```bash
./fake-webserver -num-endpoints=100 -num-regions=10 -num-versions=5
```

**Simulate 10 nodes on one process** (~48,600 time series):
```bash
./fake-webserver -num-nodes=10
```

**Extreme load test** (~156,000 time series):
```bash
./fake-webserver -num-endpoints=500 -num-regions=10 -num-versions=5
```

### Backfill mode

Setting `-backfill-start` runs a one-shot CLI instead of the web server: it
generates hours of the same load in virtual time and writes it straight to one
or more Prometheus remote-write endpoints, as fast as they accept it, then
exits.

```bash
./fake-webserver -num-nodes=14 -node-name=fake-webserver \
  -backfill-start=2026-09-28T00:00:00Z -backfill-step=15s -backfill-hours=6 \
  -backfill-labels=job=fake-webserver,namespace=perf-fakeserver \
  -remote-write=http://vm:8428/api/v1/write \
  -remote-write='http://root%40example.com:Complexpass%23123@o2:5080/api/default/prometheus/api/v1/write'
```

Backfill flags:

```
  -backfill-start string      RFC3339 time of the first point; enables backfill mode
  -backfill-step duration     interval between points (default 15s)
  -backfill-hours float       hours of data, starting at -backfill-start (default 6)
  -remote-write url           remote-write target; repeat for several. Basic auth goes
                              in the URL, percent-encoded. None = generate and count only
  -backfill-labels k=v,...    constant labels added to every series (e.g. job, instance)
  -backfill-seed int          RNG seed; same seed and flags = identical data (default 1)
  -backfill-batch-size int    samples per request (default 10000, vmagent's default)
  -backfill-senders int       concurrent requests per target (default 4)
  -backfill-encoders int      encoding goroutines (default GOMAXPROCS)
  -node-name string           base name for the vnode label (default: hostname)
```

How it relates to the live server:

- **Same traffic.** It runs the live server's load as a discrete-event
  simulation -- the same 108 (path, method) workers, request outcomes, pauses,
  oscillation and outage schedule -- on a virtual clock and one seeded RNG.
- **Exact timestamps.** Point *k* is stamped `start + k*step` and every series
  shares that timestamp, like a scraper with aligned timestamps (vmagent). Each
  point holds the state after all requests up to that instant. The simulated
  load starts one step before `start`, so a run of H hours covers
  `[start, start+H)` with `H*3600/step` points per series.
- **Identical data on every target.** Each batch is generated once and sent to
  every target byte for byte. A slow target applies backpressure instead of
  dropping data, so the run proceeds at the pace of the slowest target.
- **Failure handling.** 429, 5xx and network errors are retried with backoff;
  any other 4xx aborts the run, since skipping a batch would leave a silent hole.
  The run exits non-zero if any target's sample count differs from what was
  generated.
- **Not included:** `go_*`/`process_*` metrics, and the `up`/`scrape_*` series a
  scraper adds.

## Metrics Generated

- `codelab_api_request_duration_seconds` - Histogram of request durations (with buckets)
- `codelab_api_http_requests_in_progress` - Gauge of concurrent requests
- `codelab_api_requests_total` - Counter of total requests
- `codelab_api_request_errors_total` - Counter of failed requests

Each metric includes labels: `method`, `path`, `status`, `region`, `version`, `vnode`

## Docker image

```
openobserve/fake-webserver:v5
```

You can simple use `kubectl apply -f deploy.yaml`
