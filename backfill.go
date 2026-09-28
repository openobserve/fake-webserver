package main

// Backfill mode: generate hours of load in virtual time and push it straight to
// Prometheus remote-write endpoints, as fast as they accept it.
//
// The live server needs wall-clock hours to produce hours of data, because its
// load goroutines really sleep between requests and a scraper really waits
// between scrapes. Backfill replays the same load as a discrete-event
// simulation instead: the same 108 (path, method) workers, the same
// simulateRequest and pauseAfterRequest, the same oscillation and outage
// schedule -- driven by a virtual clock and one seeded RNG. Every -backfill-step
// of virtual time it snapshots all series and writes them with that exact
// timestamp, so points are exactly one step apart and every series in a step
// shares one timestamp -- what vmagent's aligned scrape timestamps give.
//
// The simulation starts one step before -backfill-start, so the first point is
// AT the start and a run of H hours covers [start, start+H) with H*3600/step
// points per series.
//
// Every batch is generated once and sent, byte for byte, to every target. A
// slow target applies backpressure to the generator instead of dropping data,
// so the whole run moves at the pace of the slowest target.

import (
	"bytes"
	"container/heap"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang/snappy"
	"google.golang.org/protobuf/encoding/protowire"
)

var (
	backfillStart = flag.String("backfill-start", "",
		"Backfill mode: RFC3339 timestamp of the first point, e.g. 2026-09-28T00:00:00Z. "+
			"Setting it runs a one-shot CLI instead of the web server.")
	backfillStep   = flag.Duration("backfill-step", 15*time.Second, "Backfill mode: interval between points.")
	backfillHours  = flag.Float64("backfill-hours", 6, "Backfill mode: hours of data to write, starting at -backfill-start.")
	backfillSeed   = flag.Int64("backfill-seed", 1, "Backfill mode: RNG seed. The same seed and flags produce identical data.")
	backfillLabels = flag.String("backfill-labels", "",
		"Backfill mode: constant labels added to every series, as k=v,k=v -- e.g. the job/instance/pod labels a scraper would add.")
	backfillBatch    = flag.Int("backfill-batch-size", 10000, "Backfill mode: samples per remote-write request (vmagent's default).")
	backfillSenders  = flag.Int("backfill-senders", 4, "Backfill mode: concurrent requests per target.")
	backfillEncoders = flag.Int("backfill-encoders", 0, "Backfill mode: encoding goroutines (default GOMAXPROCS).")
	backfillProgress = flag.Duration("backfill-progress", 10*time.Second, "Backfill mode: progress report interval.")

	remoteWrite stringList
)

func init() {
	flag.Var(&remoteWrite, "remote-write",
		"Backfill mode: Prometheus remote-write URL; repeat the flag for several targets. "+
			"Basic auth goes in the URL (http://user:pass@host/...), percent-encoding special characters. "+
			"With no target, the data is generated and counted but not sent.")
}

type stringList []string

func (s *stringList) String() string     { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error { *s = append(*s, v); return nil }

// ---------------------------------------------------------------------------
// Series state
// ---------------------------------------------------------------------------

const (
	metricDuration   = "codelab_api_request_duration_seconds"
	metricRequests   = "codelab_api_requests_total"
	metricErrors     = "codelab_api_request_errors_total"
	metricInProgress = "codelab_api_http_requests_in_progress"
)

// requestSeries holds one (method, path, status, region, version, vnode) label
// set: its histogram, and the requests_total counter that counts the same
// requests. It mirrors what client_golang keeps for a classic histogram --
// per-bucket counts, sum and count -- and is exported the same way.
type requestSeries struct {
	labels  [][]byte // encoded label sets: len(buckets) buckets, +Inf, _sum, _count, requests_total
	buckets []uint64 // per-bucket counts, NOT cumulative (exported cumulatively)
	sum     float64
	count   uint64
}

// counterSeries is a single series with one value: request_errors_total, or
// the http_requests_in_progress gauge (which the simulation leaves at 0, since
// no simulated request is ever in flight at a snapshot).
type counterSeries struct {
	labels []byte
	value  float64
}

type seriesStore struct {
	extra     [][2]string
	bucketLe  []string
	byKey     map[string]*requestSeries
	errors    map[string]*counterSeries
	inflight  map[string]*counterSeries
	requests  []*requestSeries // creation order, which is also export order
	errorList []*counterSeries
	inflList  []*counterSeries
}

func newSeriesStore(extra [][2]string) *seriesStore {
	le := make([]string, len(requestDurationBuckets))
	for i, b := range requestDurationBuckets {
		le[i] = formatFloat(b)
	}
	return &seriesStore{
		extra:    extra,
		bucketLe: le,
		byKey:    map[string]*requestSeries{},
		errors:   map[string]*counterSeries{},
		inflight: map[string]*counterSeries{},
	}
}

// formatFloat renders a bucket bound the way the Prometheus text exposition
// does (expfmt's writeFloat), so the `le` label values match a scrape exactly.
func formatFloat(f float64) string {
	switch {
	case f == 1:
		return "1"
	case f == 0:
		return "0"
	case math.IsInf(f, +1):
		return "+Inf"
	}
	return strconv.FormatFloat(f, 'g', -1, 64)
}

func (s *seriesStore) record(method, path string, status int, duration time.Duration, region, version, node string) {
	st := strconv.Itoa(status)

	if _, ok := s.inflight[region+"\xff"+version+"\xff"+node]; !ok {
		c := &counterSeries{labels: s.encode(metricInProgress, "region", region, "version", version, "vnode", node)}
		s.inflight[region+"\xff"+version+"\xff"+node] = c
		s.inflList = append(s.inflList, c)
	}

	key := strings.Join([]string{method, path, st, region, version, node}, "\xff")
	r, ok := s.byKey[key]
	if !ok {
		base := []string{"method", method, "path", path, "status", st, "region", region, "version", version, "vnode", node}
		r = &requestSeries{buckets: make([]uint64, len(requestDurationBuckets))}
		for _, le := range s.bucketLe {
			r.labels = append(r.labels, s.encode(metricDuration+"_bucket", append(base, "le", le)...))
		}
		r.labels = append(r.labels,
			s.encode(metricDuration+"_bucket", append(base, "le", "+Inf")...),
			s.encode(metricDuration+"_sum", base...),
			s.encode(metricDuration+"_count", base...),
			s.encode(metricRequests, base...),
		)
		s.byKey[key] = r
		s.requests = append(s.requests, r)
	}
	v := duration.Seconds()
	// client_golang's classic histogram: the first bucket whose upper bound is >= v.
	if i := sort.SearchFloat64s(requestDurationBuckets, v); i < len(r.buckets) {
		r.buckets[i]++
	}
	r.sum += v
	r.count++

	if status == http.StatusInternalServerError {
		e, ok := s.errors[key]
		if !ok {
			e = &counterSeries{labels: s.encode(metricErrors,
				"method", method, "path", path, "status", st, "region", region, "version", version, "vnode", node)}
			s.errors[key] = e
			s.errorList = append(s.errorList, e)
		}
		e.value++
	}
}

func (s *seriesStore) seriesCount() int {
	return len(s.requests)*(len(requestDurationBuckets)+4) + len(s.errorList) + len(s.inflList)
}

// encode builds the protobuf repeated-Label bytes of one series -- sorted by
// name as remote write requires, extra labels included. Series labels never
// change, so this is done once per series and reused for every point.
func (s *seriesStore) encode(name string, kv ...string) []byte {
	pairs := make([][2]string, 0, len(kv)/2+1+len(s.extra))
	pairs = append(pairs, [2]string{"__name__", name})
	for i := 0; i+1 < len(kv); i += 2 {
		pairs = append(pairs, [2]string{kv[i], kv[i+1]})
	}
	pairs = append(pairs, s.extra...)
	sort.Slice(pairs, func(i, j int) bool { return pairs[i][0] < pairs[j][0] })

	var out []byte
	for _, p := range pairs {
		var l []byte
		l = protowire.AppendTag(l, 1, protowire.BytesType)
		l = protowire.AppendString(l, p[0])
		l = protowire.AppendTag(l, 2, protowire.BytesType)
		l = protowire.AppendString(l, p[1])
		out = protowire.AppendTag(out, 1, protowire.BytesType) // TimeSeries.labels
		out = protowire.AppendBytes(out, l)
	}
	return out
}

// batch is a slice of one snapshot: up to -backfill-batch-size series, all at
// one timestamp. Values are copied at snapshot time; label bytes are shared.
type batch struct {
	ts     int64 // milliseconds
	labels [][]byte
	values []float64
}

// snapshot emits every series' current value at ts, cut into batches.
func (s *seriesStore) snapshot(ts int64, size int, out chan<- *batch) int {
	b := &batch{ts: ts}
	n := 0
	add := func(l []byte, v float64) {
		b.labels = append(b.labels, l)
		b.values = append(b.values, v)
		n++
		if len(b.labels) >= size {
			out <- b
			b = &batch{ts: ts}
		}
	}
	for _, r := range s.requests {
		var cum uint64
		for i, c := range r.buckets {
			cum += c
			add(r.labels[i], float64(cum))
		}
		nb := len(r.buckets)
		add(r.labels[nb], float64(r.count))   // le="+Inf"
		add(r.labels[nb+1], r.sum)            // _sum
		add(r.labels[nb+2], float64(r.count)) // _count
		add(r.labels[nb+3], float64(r.count)) // requests_total: one per request, like _count
	}
	for _, e := range s.errorList {
		add(e.labels, e.value)
	}
	for _, g := range s.inflList {
		add(g.labels, g.value)
	}
	if len(b.labels) > 0 {
		out <- b
	}
	return n
}

// encodeWriteRequest renders a batch as a snappy-compressed remote-write
// WriteRequest: one TimeSeries per series, each with a single sample.
func encodeWriteRequest(b *batch, buf []byte) (raw []byte, compressed []byte) {
	buf = buf[:0]
	var sample [32]byte
	for i, l := range b.labels {
		sm := sample[:0]
		sm = protowire.AppendTag(sm, 1, protowire.Fixed64Type)
		sm = protowire.AppendFixed64(sm, math.Float64bits(b.values[i]))
		sm = protowire.AppendTag(sm, 2, protowire.VarintType)
		sm = protowire.AppendVarint(sm, uint64(b.ts))

		tsLen := len(l) + protowire.SizeTag(2) + protowire.SizeBytes(len(sm))
		buf = protowire.AppendTag(buf, 1, protowire.BytesType) // WriteRequest.timeseries
		buf = protowire.AppendVarint(buf, uint64(tsLen))
		buf = append(buf, l...)
		buf = protowire.AppendTag(buf, 2, protowire.BytesType) // TimeSeries.samples
		buf = protowire.AppendBytes(buf, sm)
	}
	return buf, snappy.Encode(nil, buf)
}

// ---------------------------------------------------------------------------
// The virtual-time load
// ---------------------------------------------------------------------------

// worker is one load goroutine of the live server: a fixed (path, method),
// issuing a request, pausing, and repeating.
type worker struct {
	id     int
	path   string
	method string
	next   time.Duration // virtual time of its next request, since the load started
}

type workerHeap []*worker

func (h workerHeap) Len() int { return len(h) }
func (h workerHeap) Less(i, j int) bool {
	if h[i].next != h[j].next {
		return h[i].next < h[j].next
	}
	return h[i].id < h[j].id
}
func (h workerHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *workerHeap) Push(x any)   { *h = append(*h, x.(*worker)) }
func (h *workerHeap) Pop() any {
	old := *h
	w := old[len(old)-1]
	*h = old[:len(old)-1]
	return w
}

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

type payload struct {
	data    []byte
	samples int
}

type target struct {
	name     string // URL without credentials, for logs
	url      string
	user     string
	pass     string
	hasAuth  bool
	ch       chan *payload
	samples  atomic.Int64
	bytes    atomic.Int64
	requests atomic.Int64
	retries  atomic.Int64
}

func parseTarget(raw string, senders int) (*target, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("remote-write %q: scheme must be http or https", raw)
	}
	t := &target{ch: make(chan *payload, 4*senders)}
	if u.User != nil {
		t.user = u.User.Username()
		t.pass, _ = u.User.Password()
		t.hasAuth = true
		u.User = nil
	}
	t.url = u.String()
	t.name = u.Host + u.Path
	return t, nil
}

func (t *target) send(client *http.Client, p *payload) {
	backoff := 250 * time.Millisecond
	for attempt := 1; ; attempt++ {
		code, body, err := t.post(client, p.data)
		if err == nil && code/100 == 2 {
			t.samples.Add(int64(p.samples))
			t.bytes.Add(int64(len(p.data)))
			t.requests.Add(1)
			return
		}
		// A 4xx other than 429 is the target rejecting the data itself;
		// retrying will not change the answer, and skipping it would leave a
		// silent hole in the dataset.
		if err == nil && code/100 == 4 && code != http.StatusTooManyRequests {
			log.Fatalf("backfill: %s rejected a batch: HTTP %d: %s", t.name, code, body)
		}
		t.retries.Add(1)
		if attempt == 1 || attempt%10 == 0 {
			if err != nil {
				log.Printf("backfill: %s: %v (attempt %d, retrying)", t.name, err, attempt)
			} else {
				log.Printf("backfill: %s: HTTP %d: %s (attempt %d, retrying)", t.name, code, body, attempt)
			}
		}
		time.Sleep(backoff)
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (t *target) post(client *http.Client, data []byte) (int, string, error) {
	req, err := http.NewRequest(http.MethodPost, t.url, bytes.NewReader(data))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Encoding", "snappy")
	req.Header.Set("Content-Type", "application/x-protobuf")
	req.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
	req.Header.Set("User-Agent", "fake-webserver-backfill")
	if t.hasAuth {
		req.SetBasicAuth(t.user, t.pass)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	return resp.StatusCode, strings.TrimSpace(string(body)), nil
}

// ---------------------------------------------------------------------------
// Run
// ---------------------------------------------------------------------------

func parseLabels(s string) ([][2]string, error) {
	var out [][2]string
	if strings.TrimSpace(s) == "" {
		return out, nil
	}
	for _, kv := range strings.Split(s, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(kv), "=")
		if !ok || k == "" {
			return nil, fmt.Errorf("-backfill-labels: %q is not k=v", kv)
		}
		out = append(out, [2]string{k, v})
	}
	return out, nil
}

func runBackfill() error {
	startTS, err := time.Parse(time.RFC3339, *backfillStart)
	if err != nil {
		return fmt.Errorf("-backfill-start: %w", err)
	}
	step := *backfillStep
	if step <= 0 {
		return errors.New("-backfill-step must be positive")
	}
	span := time.Duration(*backfillHours * float64(time.Hour))
	if span <= 0 || span%step != 0 {
		return fmt.Errorf("-backfill-hours (%v) must be a positive whole number of steps (%v)", span, step)
	}
	points := int(span / step)
	extra, err := parseLabels(*backfillLabels)
	if err != nil {
		return err
	}
	if *backfillBatch <= 0 || *backfillSenders <= 0 {
		return errors.New("-backfill-batch-size and -backfill-senders must be positive")
	}
	encoders := *backfillEncoders
	if encoders <= 0 {
		encoders = runtime.GOMAXPROCS(0)
	}

	var targets []*target
	for _, raw := range remoteWrite {
		t, err := parseTarget(raw, *backfillSenders)
		if err != nil {
			return err
		}
		targets = append(targets, t)
	}

	rng := rand.New(rand.NewSource(*backfillSeed))
	apiEndpoints = generateEndpoints(*numEndpoints, rng)
	regions := generateRegions(*numRegions)
	versions := generateVersions(*numVersions)
	nodes := generateNodes(*numNodes)
	printEstimate(apiEndpoints, regions, versions, nodes)

	last := startTS.Add(time.Duration(points-1) * step)
	fmt.Printf("Backfill:\n")
	fmt.Printf("  first point      %s\n", startTS.UTC().Format(time.RFC3339))
	fmt.Printf("  last point       %s\n", last.UTC().Format(time.RFC3339))
	fmt.Printf("  step             %v\n", step)
	fmt.Printf("  points/series    %d  (%v)\n", points, span)
	fmt.Printf("  seed             %d\n", *backfillSeed)
	fmt.Printf("  extra labels     %v\n", extra)
	fmt.Printf("  batch/senders    %d samples, %d per target, %d encoders\n", *backfillBatch, *backfillSenders, encoders)
	if len(targets) == 0 {
		fmt.Printf("  targets          none -- generating and counting only\n")
	}
	for _, t := range targets {
		fmt.Printf("  target           %s\n", t.name)
	}
	if last.After(time.Now()) {
		fmt.Printf("  WARNING          the last point is in the future\n")
	}
	fmt.Println()

	// Workers: one per (path, method), in a fixed order so a seed is reproducible.
	paths := make([]string, 0, len(apiEndpoints))
	for p := range apiEndpoints {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	h := &workerHeap{}
	for _, p := range paths {
		for _, m := range httpMethods {
			heap.Push(h, &worker{id: h.Len(), path: p, method: m})
		}
	}

	store := newSeriesStore(extra)

	// Pipeline: simulate -> batches -> encoders -> payloads -> every target.
	batches := make(chan *batch, 2*encoders)
	payloads := make(chan *payload, 2*encoders)
	var encWG sync.WaitGroup
	for i := 0; i < encoders; i++ {
		encWG.Add(1)
		go func() {
			defer encWG.Done()
			var buf []byte
			for b := range batches {
				var comp []byte
				buf, comp = encodeWriteRequest(b, buf)
				payloads <- &payload{data: comp, samples: len(b.labels)}
			}
		}()
	}
	var generated, generatedBytes atomic.Int64
	var dispWG sync.WaitGroup
	dispWG.Add(1)
	go func() {
		defer dispWG.Done()
		for p := range payloads {
			generated.Add(int64(p.samples))
			generatedBytes.Add(int64(len(p.data)))
			for _, t := range targets {
				t.ch <- p
			}
		}
		for _, t := range targets {
			close(t.ch)
		}
	}()
	client := &http.Client{
		Timeout: 2 * time.Minute,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *backfillSenders,
			IdleConnTimeout:     90 * time.Second,
		},
	}
	var sendWG sync.WaitGroup
	for _, t := range targets {
		for i := 0; i < *backfillSenders; i++ {
			sendWG.Add(1)
			go func(t *target) {
				defer sendWG.Done()
				for p := range t.ch {
					t.send(client, p)
				}
			}(t)
		}
	}

	// Progress.
	began := time.Now()
	var stepsDone atomic.Int64
	var seriesNow atomic.Int64
	stopProgress := make(chan struct{})
	progressDone := make(chan struct{})
	go func() {
		defer close(progressDone)
		tick := time.NewTicker(*backfillProgress)
		defer tick.Stop()
		prevT := began
		prev := map[*target]int64{}
		prevGen := int64(0)
		for {
			select {
			case <-stopProgress:
				return
			case now := <-tick.C:
				dt := now.Sub(prevT).Seconds()
				prevT = now
				sd := stepsDone.Load()
				gen := generated.Load()
				line := fmt.Sprintf("[%s] step %d/%d (%.1f%%) at %s  series %d  generated %s (%s/s)",
					fmtDur(now.Sub(began)), sd, points, 100*float64(sd)/float64(points),
					startTS.Add(time.Duration(max64(sd-1, 0))*step).UTC().Format("15:04:05"),
					seriesNow.Load(), human(gen), human(int64(float64(gen-prevGen)/dt)))
				prevGen = gen
				var slowest *target
				for _, t := range targets {
					s := t.samples.Load()
					line += fmt.Sprintf("  | %s %s (%s/s)", shortName(t.name), human(s), human(int64(float64(s-prev[t])/dt)))
					if r := t.retries.Load(); r > 0 {
						line += fmt.Sprintf(" retries=%d", r)
					}
					prev[t] = s
					if slowest == nil || s < slowest.samples.Load() {
						slowest = t
					}
				}
				// ETA from the slowest target against the projected total.
				if sd > 0 {
					total := float64(gen) / float64(sd) * float64(points)
					done := float64(gen)
					if slowest != nil {
						done = float64(slowest.samples.Load())
					}
					if el := now.Sub(began).Seconds(); done > 0 {
						line += fmt.Sprintf("  eta %s", fmtDur(time.Duration((total-done)/(done/el)*float64(time.Second))))
					}
				}
				fmt.Println(line)
			}
		}
	}()

	// The simulation. The load starts one step before the first point, so the
	// point at startTS already holds one step of traffic.
	var expected int64
	for k := 0; k < points; k++ {
		boundary := time.Duration(k+1) * step // virtual time since the load started
		for (*h)[0].next <= boundary {
			w := (*h)[0]
			el := w.next
			region := regions[rng.Intn(len(regions))]
			version := versions[rng.Intn(len(versions))]
			node := nodes[rng.Intn(len(nodes))]
			status, duration := simulateRequest(w.method, w.path, el, rng)
			store.record(w.method, w.path, status, duration, region, version, node)
			w.next = el + pauseAfterRequest(el, rng)
			heap.Fix(h, 0)
		}
		ts := startTS.Add(time.Duration(k) * step).UnixMilli()
		expected += int64(store.snapshot(ts, *backfillBatch, batches))
		seriesNow.Store(int64(store.seriesCount()))
		stepsDone.Store(int64(k + 1))
	}
	close(batches)
	encWG.Wait()
	close(payloads)
	dispWG.Wait()
	sendWG.Wait()
	close(stopProgress)
	<-progressDone

	wall := time.Since(began)
	fmt.Printf("\nBackfill done in %s\n", fmtDur(wall))
	fmt.Printf("  points           %d per series, %s .. %s\n", points,
		startTS.UTC().Format(time.RFC3339), last.UTC().Format(time.RFC3339))
	fmt.Printf("  series           %d at the last point\n", store.seriesCount())
	fmt.Printf("  samples          %d generated (%s compressed)\n", generated.Load(), humanBytes(generatedBytes.Load()))
	ok := generated.Load() == expected
	for _, t := range targets {
		s := t.samples.Load()
		fmt.Printf("  %-16s %d samples, %d requests, %s, %d retries, %s samples/s\n",
			shortName(t.name), s, t.requests.Load(), humanBytes(t.bytes.Load()), t.retries.Load(),
			human(int64(float64(s)/wall.Seconds())))
		if s != expected {
			ok = false
		}
	}
	if !ok {
		return fmt.Errorf("sample counts disagree: expected %d", expected)
	}
	return nil
}

func shortName(n string) string {
	if i := strings.IndexByte(n, '/'); i > 0 {
		n = n[:i]
	}
	return n
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func human(n int64) string {
	f := float64(n)
	switch {
	case f >= 1e9:
		return fmt.Sprintf("%.2fB", f/1e9)
	case f >= 1e6:
		return fmt.Sprintf("%.2fM", f/1e6)
	case f >= 1e3:
		return fmt.Sprintf("%.1fk", f/1e3)
	}
	return strconv.FormatInt(n, 10)
}

func humanBytes(n int64) string {
	f := float64(n)
	switch {
	case f >= 1<<30:
		return fmt.Sprintf("%.2f GiB", f/(1<<30))
	case f >= 1<<20:
		return fmt.Sprintf("%.1f MiB", f/(1<<20))
	}
	return fmt.Sprintf("%d B", n)
}

func fmtDur(d time.Duration) string {
	d = d.Round(time.Second)
	return fmt.Sprintf("%02d:%02d:%02d", int(d.Hours()), int(d.Minutes())%60, int(d.Seconds())%60)
}
