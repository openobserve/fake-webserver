# fake-webserver

It's a Fake Webserver to generate metrics data for Prometheus load testing.

## Features

- Generates configurable number of API endpoints
- Creates multiple label combinations (regions, versions) for high cardinality testing
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

**Extreme load test** (~156,000 time series):
```bash
./fake-webserver -num-endpoints=500 -num-regions=10 -num-versions=5
```

## Metrics Generated

- `codelab_api_request_duration_seconds` - Histogram of request durations (with buckets)
- `codelab_api_http_requests_in_progress` - Gauge of concurrent requests
- `codelab_api_requests_total` - Counter of total requests
- `codelab_api_request_errors_total` - Counter of failed requests

Each metric includes labels: `method`, `path`, `status`, `region`, `version`

## Docker image

```
openobserve/fake-webserver:v2
```

You can simple use `kubectl apply -f deploy.yaml`
