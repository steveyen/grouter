package main

import (
	"flag"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/steveyen/grouter"
)

type EndPoint struct {
	Usage string // Help string.
	Start func(string, grouter.Params, chan []grouter.Request, chan grouter.Stats)
	MaxConcurrency int // Some end-points have limited concurrency.
}

// Available sources of requests.
var Sources = map[string]EndPoint{
	"memcached": EndPoint{
		"memcached:LISTEN_INTERFACE:LISTEN_PORT",
		grouter.MakeListenSourceFunc(&grouter.AsciiSource{}), 0,
	},
	"memcached-ascii": EndPoint{
		"memcached-ascii:LISTEN_INTERFACE:LISTEN_PORT",
		grouter.MakeListenSourceFunc(&grouter.AsciiSource{}), 0,
	},
	"workload": EndPoint{"workload", grouter.WorkLoad, 0},
}

// Available targets of requests.
var Targets = map[string]EndPoint{
	"http": EndPoint{
		"http://COUCHBASE_HOST:COUCHBASE_PORT",
        grouter.CouchbaseTargetRun, 0,
	},
	"couchbase": EndPoint{
		"couchbase://COUCHBASE_HOST:COUCHBASE_PORT",
        grouter.CouchbaseTargetRun, 0,
	},
	"memcached-ascii": EndPoint{
		"memcached-ascii:HOST:PORT",
		grouter.MemcachedAsciiTargetRun, 0,
	},
	"memcached-binary": EndPoint{
		"memcached-binary:HOST:PORT",
		grouter.MemcachedBinaryTargetRun, 0,
	},
	"memory": EndPoint{"memory", grouter.MemoryStorageRun, 1},
}

func main() {
	sourceSpec := flag.String("source", "memcached-ascii::11300",
        "source of requests\n" +
        "    as SOURCE_KIND[:MORE_PARAMS]\n" +
        "    examples..." + EndPointExamples(Sources))
	sourceMaxConns := flag.Int("source-max-conns", 100,
		"max conns allowed from source")

	targetSpec := flag.String("target", "memory",
        "target of requests\n" +
        "    as TARGET_KIND[:MORE_PARAMS]\n" +
        "    examples..." + EndPointExamples(Targets))
	targetChanSize := flag.Int("target-chan-size", 5,
		"target chan size to control queuing")
	targetConcurrency := flag.Int("target-concurrency", 4,
		"# of concurrent workers in front of target")

	flag.Parse()

	params := grouter.Params{
		SourceSpec:        *sourceSpec,
		SourceMaxConns:    *sourceMaxConns,
		TargetSpec:        *targetSpec,
		TargetChanSize:    *targetChanSize,
		TargetConcurrency: *targetConcurrency,
	}

	log.Printf("grouter")
	log.Printf("  source: %v", params.SourceSpec)
	log.Printf("    source-max-conns: %v", params.SourceMaxConns)
	log.Printf("  target: %v", params.TargetSpec)
	log.Printf("    target-chan-size: %v", params.TargetChanSize)
	log.Printf("    target-concurrency: %v", params.TargetConcurrency)

	MainStart(params)
}

func MainStart(params grouter.Params) {
	sourceKind := strings.Split(params.SourceSpec, ":")[0]
	if source, ok := Sources[sourceKind]; ok {
		targetKind := strings.Split(params.TargetSpec, ":")[0]
		if target, ok := Targets[targetKind]; ok {
			// The source will send requests to a shared, unbatched channel.
			unbatched := make(chan []grouter.Request, params.TargetChanSize)

			targetConcurrency := params.TargetConcurrency
			if (target.MaxConcurrency > 0 &&
				target.MaxConcurrency < targetConcurrency) {
				targetConcurrency = target.MaxConcurrency
				log.Printf("    target-concurrency clipped to: %v;" +
					" due to limitations of target kind: %v",
					targetConcurrency, targetKind)
			}

			statsChan := StartStatsReporter(params.SourceMaxConns + params.TargetConcurrency)

			// Requests coming into the shared, unbatched channel are
			// partitioned, into concurrent "lanes", where target
			// goroutines are assigned to each own its own lane (channel).
			lanes := make([]chan []grouter.Request, targetConcurrency)
			for i := range(lanes) {
				lanes[i] = make(chan []grouter.Request, params.TargetChanSize)
				StartTarget(target, lanes[i], params, statsChan)
			}
			go grouter.PartitionRequests(unbatched, lanes)

			// Start the source after all the lanes and channels are setup.
			source.Start(params.SourceSpec, params, unbatched, statsChan)
		} else {
			log.Fatalf("error: unknown target kind: %s", params.TargetSpec)
		}
	} else {
		log.Fatalf("error: unknown source kind: %s", params.SourceSpec)
	}
}

func StartTarget(target EndPoint, unbatched chan[]grouter.Request,
	params grouter.Params, statsChan chan grouter.Stats) {
	batched := make(chan []grouter.Request, params.TargetChanSize)
	go func() {
		grouter.BatchRequests(params.TargetChanSize, unbatched, batched)
	}()
	go func() {
		target.Start(params.TargetSpec, params, batched, statsChan)
	}()
}

func StartStatsReporter(chanSize int) chan grouter.Stats {
	reportChan := time.Tick(2 * time.Second)
	statsChan := make(chan grouter.Stats, chanSize)
	curr := make(map[string] int64)
	prev := make(map[string] int64)

	go func() {
		for {
			select {
			case stats := <-statsChan:
				for i := range stats.Keys {
					curr[stats.Keys[i]] += stats.Vals[i]
				}
			case <-reportChan:
				StatsReport(curr, prev)
				for k, v := range(curr) {
					prev[k] = v
				}
			}
		}
	}()

	return statsChan
}

func StatsReport(curr map[string] int64, prev map[string] int64) {
	// Reports rates on paired stats that follow a naming convention
	// like xxx and xxx_usecs.  For example, tot_ops and tot_ops_usecs.
	for k, v := range(curr) {
		k_usecs := k + "_usecs"
		v_usecs := curr[k_usecs]
		if v_usecs > 0 {
			k_per_usec := float64(v - prev[k]) / float64(v_usecs - prev[k_usecs])
			log.Printf("%v/sec = %f", k, k_per_usec * 1000000.0)
		}
	}
}

func EndPointExamples(m map[string]EndPoint) (rv string) {
	mk := make([]string, len(m))
	i := 0
    for k, _ := range m {
		mk[i] = k
		i++
    }
    sort.Strings(mk)
	rv = ""
	for _, s := range mk {
		rv = rv + "\n      " + m[s].Usage
	}
	return rv
}
