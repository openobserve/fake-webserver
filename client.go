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
	"time"
)

var oscillationPeriod = flag.Duration("oscillation-period", 5*time.Minute, "The duration of the rate oscillation period.")

func runClient() {
	oscillationFactor := func() float64 {
		return 2 + math.Sin(math.Sin(2*math.Pi*float64(time.Since(start))/float64(*oscillationPeriod)))
	}

	// Generate dynamic endpoints, regions, versions, and nodes
	endpoints := generateEndpoints(*numEndpoints)
	regions := generateRegions(*numRegions)
	versions := generateVersions(*numVersions)
	nodes := generateNodes(*numNodes)

	fmt.Printf("Starting load generation with:\n")
	fmt.Printf("  - %d API endpoints\n", len(endpoints))
	fmt.Printf("  - %d regions\n", len(regions))
	fmt.Printf("  - %d versions\n", len(versions))
	fmt.Printf("  - %d nodes\n", len(nodes))
	fmt.Printf("  - Estimated unique time series: ~%d\n", len(endpoints)*2*len(regions)*len(versions)*len(nodes)*3) // methods * labels * statuses

	// Generate load for each endpoint with different regions, versions, and nodes
	for path := range endpoints {
		for _, method := range []string{"GET", "POST"} {
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
