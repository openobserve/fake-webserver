// Copyright 2019 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"fmt"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	registry = prometheus.NewRegistry()

	namespace = "codelab"
	subsystem = "api"

	requestHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_duration_seconds",
			Help:      "A histogram of the API HTTP request durations in seconds.",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 25),
		},
		[]string{"method", "path", "status", "region", "version", "node"},
	)
	requestsInProgress = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "http_requests_in_progress",
			Help:      "The current number of API HTTP requests in progress.",
		},
		[]string{"region", "version", "node"},
	)
	requestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "requests_total",
			Help:      "Total number of requests",
		},
		[]string{"method", "path", "status", "region", "version", "node"},
	)
	requestErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "request_errors_total",
			Help:      "Total number of request errors",
		},
		[]string{"method", "path", "status", "region", "version", "node"},
	)
)

func init() {
	registry.MustRegister(
		requestsTotal,
		requestErrorsTotal,
		requestHistogram,
		requestsInProgress,
	)
}

type responseOpts struct {
	baseLatency time.Duration
	errorRatio  float64

	// Whenever 10*outageDuration has passed, an outage will be simulated
	// that lasts for outageDuration. During the outage, errorRatio is
	// increased by a factor of 10, and baseLatency by a factor of 3.  At
	// start-up time, an outage is simulated, too (so that you can see the
	// effects right ahead and don't have to wait for 10*outageDuration).
	outageDuration time.Duration
}

var opts = map[string]map[string]responseOpts{
	"/api/foo": {
		"GET": {
			baseLatency:    10 * time.Millisecond,
			errorRatio:     0.005,
			outageDuration: 23 * time.Second,
		},
		"POST": {
			baseLatency:    20 * time.Millisecond,
			errorRatio:     0.02,
			outageDuration: time.Minute,
		},
	},
	"/api/bar": {
		"GET": {
			baseLatency:    15 * time.Millisecond,
			errorRatio:     0.0025,
			outageDuration: 13 * time.Second,
		},
		"POST": {
			baseLatency:    50 * time.Millisecond,
			errorRatio:     0.01,
			outageDuration: 47 * time.Second,
		},
	},
	"/api/baz": {
		"GET": {
			baseLatency:    2 * time.Millisecond,
			errorRatio:     0.01,
			outageDuration: 1 * time.Second,
		},
		"POST": {
			baseLatency:    4 * time.Millisecond,
			errorRatio:     0.02,
			outageDuration: 2 * time.Second,
		},
	},
	"/api/boom": {
		"GET": {
			baseLatency:    5 * time.Millisecond,
			errorRatio:     0.01,
			outageDuration: 1 * time.Second,
		},
		"POST": {
			baseLatency:    14 * time.Millisecond,
			errorRatio:     0.02,
			outageDuration: 2 * time.Second,
		},
	},
}

// generateEndpoints creates dynamic API endpoint configurations based on numEndpoints flag
func generateEndpoints(numEndpoints int) map[string]map[string]responseOpts {
	endpoints := make(map[string]map[string]responseOpts)

	// Keep the original endpoints for backward compatibility
	for path, methods := range opts {
		endpoints[path] = methods
	}

	// Generate additional endpoints
	for i := 1; i <= numEndpoints; i++ {
		path := fmt.Sprintf("/api/service-%d", i)
		endpoints[path] = map[string]responseOpts{
			"GET": {
				baseLatency:    time.Duration(5+rand.Intn(20)) * time.Millisecond,
				errorRatio:     0.001 + rand.Float64()*0.02,
				outageDuration: time.Duration(10+rand.Intn(50)) * time.Second,
			},
			"POST": {
				baseLatency:    time.Duration(10+rand.Intn(40)) * time.Millisecond,
				errorRatio:     0.005 + rand.Float64()*0.03,
				outageDuration: time.Duration(20+rand.Intn(100)) * time.Second,
			},
		}
	}

	return endpoints
}

// generateRegions creates region labels
func generateRegions(numRegions int) []string {
	regions := []string{}
	regionNames := []string{"us-east", "us-west", "eu-west", "eu-central", "ap-southeast", "ap-northeast", "sa-east", "ca-central"}

	for i := 0; i < numRegions && i < len(regionNames)*3; i++ {
		region := fmt.Sprintf("%s-%d", regionNames[i%len(regionNames)], (i/len(regionNames))+1)
		regions = append(regions, region)
	}

	return regions
}

// generateVersions creates version labels
func generateVersions(numVersions int) []string {
	versions := []string{}

	for i := 0; i < numVersions; i++ {
		version := fmt.Sprintf("v1.%d.0", i)
		versions = append(versions, version)
	}

	return versions
}

// generateNodes creates node labels from this host's name.
// With numNodes == 1, the label is the hostname as-is.
// With numNodes > 1, simulated names are derived from the hostname (e.g. host-1, host-2).
func generateNodes(numNodes int) []string {
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "unknown"
	}
	if numNodes <= 1 {
		return []string{hostname}
	}

	nodes := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = fmt.Sprintf("%s-%d", hostname, i+1)
	}
	return nodes
}

func handleAPI(method, path, region, version, node string) {
	requestsInProgress.With(prometheus.Labels{
		"region":  region,
		"version": version,
		"node":    node,
	}).Inc()
	status := http.StatusOK
	duration := time.Millisecond

	defer func() {
		requestsInProgress.With(prometheus.Labels{
			"region":  region,
			"version": version,
			"node":    node,
		}).Dec()
		requestHistogram.With(prometheus.Labels{
			"method":  method,
			"path":    path,
			"status":  fmt.Sprint(status),
			"region":  region,
			"version": version,
			"node":    node,
		}).Observe(duration.Seconds())
		requestsTotal.WithLabelValues(method, path, fmt.Sprint(status), region, version, node).Inc()
	}()

	pathOpts, ok := opts[path]
	if !ok {
		status = http.StatusNotFound
		return
	}
	methodOpts, ok := pathOpts[method]
	if !ok {
		status = http.StatusMethodNotAllowed
		return
	}
	latencyFactor := time.Duration(1)
	errorFactor := 1.
	if time.Since(start)%(10*methodOpts.outageDuration) < methodOpts.outageDuration {
		latencyFactor *= 3
		errorFactor *= 10
	}
	duration = (methodOpts.baseLatency + time.Duration(rand.NormFloat64()*float64(methodOpts.baseLatency)/10)) * latencyFactor

	if rand.Float64() <= methodOpts.errorRatio*errorFactor {
		status = http.StatusInternalServerError
		requestErrorsTotal.WithLabelValues(method, path, fmt.Sprint(status), region, version, node).Inc()
	}
}
