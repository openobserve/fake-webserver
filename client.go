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
	"flag"
	"fmt"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

var oscillationPeriod = flag.Duration("oscillation-period", 5*time.Minute, "The duration of the rate oscillation period.")

// httpMethods are the methods each endpoint is driven with.
var httpMethods = []string{"GET", "POST"}

func runClient() {
	oscillationFactor := func() float64 {
		return 2 + math.Sin(math.Sin(2*math.Pi*float64(time.Since(start))/float64(*oscillationPeriod)))
	}

	// Generate dynamic endpoints, regions, versions, and nodes. handleAPI
	// serves from apiEndpoints, so it has to be set before any load starts.
	apiEndpoints = generateEndpoints(*numEndpoints)
	endpoints := apiEndpoints
	regions := generateRegions(*numRegions)
	versions := generateVersions(*numVersions)
	nodes := generateNodes(*numNodes)

	printEstimate(endpoints, regions, versions, nodes)

	// Generate load for each endpoint with different regions, versions, and nodes
	for path := range endpoints {
		for _, method := range httpMethods {
			// Create a copy for the closure
			currentPath := path
			currentMethod := method

			go func() {
				for {
					// Randomly select region, version, and node to create diverse series
					region := regions[rand.Intn(len(regions))]
					version := versions[rand.Intn(len(versions))]
					node := nodes[rand.Intn(len(nodes))]

					handleAPI(currentMethod, currentPath, region, version, node)

					// Variable sleep time based on oscillation
					sleepTime := time.Duration(float64(5+rand.Intn(50)) * oscillationFactor())
					time.Sleep(sleepTime * time.Millisecond)
				}
			}()
		}
	}

	select {}
}

// printEstimate reports how many time series the configured load will produce.
// The count is driven by the number of distinct label sets, which each metric
// then expands differently: a histogram costs one series per bucket plus +Inf,
// _sum and _count, while the counters and the gauge cost one series each.
func printEstimate(endpoints map[string]map[string]responseOpts, regions, versions, nodes []string) {
	pathSets := len(endpoints) * len(httpMethods) * statusesPerEndpoint
	rvn := len(regions) * len(versions) * len(nodes)
	labelSets := pathSets * rvn

	seriesPerHistogram := len(requestDurationBuckets) + 3 // buckets + +Inf + _sum + _count
	histogramSeries := labelSets * seriesPerHistogram
	errorSets := len(endpoints) * len(httpMethods) // errors only ever carry the 500 status
	errorSeries := errorSets * rvn
	total := histogramSeries + labelSets + errorSeries + rvn

	fmt.Printf("Starting load generation with:\n")
	fmt.Printf("  - %d API endpoints\n", len(endpoints))
	fmt.Printf("  - %d regions\n", len(regions))
	fmt.Printf("  - %d versions\n", len(versions))
	fmt.Printf("  - %d nodes\n", len(nodes))

	fmt.Printf("\nEstimated unique time series:\n\n")
	fmt.Printf("  label sets = %d * %d * %d * %d * %d * %d = %d * %d = %d\n\n",
		len(endpoints), len(httpMethods), statusesPerEndpoint,
		len(regions), len(versions), len(nodes),
		pathSets, rvn, labelSets)

	width := len(strconv.Itoa(total))
	row := func(name, expr string, value int) {
		sep := "="
		if expr == "" {
			sep = " "
		}
		fmt.Printf("  %-16s %16s %s %*d\n", name, expr, sep, width, value)
	}
	row("histogram", fmt.Sprintf("%d x %d", labelSets, seriesPerHistogram), histogramSeries)
	row("requests_total", "", labelSets)
	row("errors_total", fmt.Sprintf("%d x %d", errorSets, rvn), errorSeries)
	row("in_progress", "", rvn)
	fmt.Printf("  %-16s %16s   %s\n", "", "", strings.Repeat("-", width))
	row("total", "", total)
	fmt.Println()
}
