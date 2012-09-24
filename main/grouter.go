package main

import (
	"flag"
	"log"
	"sort"
	"strings"

	"github.com/steveyen/grouter"
)

type EndPoint struct {
	Usage string // Help string.
	RunSource func(string, grouter.Params, grouter.Target, chan grouter.Stats)
	StartTarget func(string, grouter.Params, chan grouter.Stats) grouter.Target
	MaxConcurrency int // Some end-points have limited concurrency.
}

// Available sources of requests.
var Sources = map[string]EndPoint{
	"memcached": EndPoint{
		Usage: "memcached:LISTEN_INTERFACE:LISTEN_PORT",
		RunSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"memcached-ascii": EndPoint{
		Usage: "memcached-ascii:LISTEN_INTERFACE:LISTEN_PORT",
		RunSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"workload": EndPoint{
		Usage: "workload",
		RunSource: grouter.WorkLoadRun,
	},
}

// Available targets of requests.
var Targets = map[string]EndPoint{
	"http": EndPoint{
		Usage: "http://COUCHBASE_HOST:COUCHBASE_PORT",
        StartTarget: grouter.CouchbaseTargetStart,
	},
	"couchbase": EndPoint{
		Usage: "couchbase://COUCHBASE_HOST:COUCHBASE_PORT",
        StartTarget: grouter.CouchbaseTargetStart,
	},
	"memcached-ascii": EndPoint{
		Usage: "memcached-ascii:HOST:PORT",
		StartTarget: grouter.MemcachedAsciiTargetStart,
	},
	"memcached-binary": EndPoint{
		Usage: "memcached-binary:HOST:PORT",
		StartTarget: grouter.MemcachedBinaryTargetStart,
	},
	"memory": EndPoint{
		Usage: "memory",
		StartTarget: grouter.MemoryStorageStart,
		MaxConcurrency: 1,
	},
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
	if sourceDef, ok := Sources[sourceKind]; ok {
		targetKind := strings.Split(params.TargetSpec, ":")[0]
		if targetDef, ok := Targets[targetKind]; ok {
			if (targetDef.MaxConcurrency > 0 &&
				targetDef.MaxConcurrency < params.TargetConcurrency) {
				params.TargetConcurrency = targetDef.MaxConcurrency
				log.Printf("    target-concurrency clipped to: %v;" +
					" due to limitations of target kind: %v",
					params.TargetConcurrency, targetKind)
			}

			statsChan := grouter.StartStatsReporter(
				params.SourceMaxConns + params.TargetConcurrency)

			target := targetDef.StartTarget(params.TargetSpec, params, statsChan)

			sourceDef.RunSource(params.SourceSpec, params, target, statsChan)

			// --------------------
			/*
			targetChans := make([]chan []grouter.Request, params.TargetConcurrency)
			for i := range(targetChans) {
				targetChans[i] = make(chan []grouter.Request, params.TargetChanSize)
				StartTarget(target, targetChans[i], params, statsChan)
			}

			sourceDef.StartSource(params.SourceSpec, params, targetChans, statsChan)
			 */
			// --------------------
		} else {
			log.Fatalf("error: unknown target kind: %s", params.TargetSpec)
		}
	} else {
		log.Fatalf("error: unknown source kind: %s", params.SourceSpec)
	}
}

/*
func StartTarget(target EndPoint, unbatched chan[]grouter.Request,
	params grouter.Params, statsChan chan grouter.Stats) {
	batched := make(chan []grouter.Request, params.TargetChanSize)
	go func() {
		grouter.BatchRequests(params.TargetChanSize, unbatched, batched, statsChan)
	}()
	go func() {
		target.StartTarget(params.TargetSpec, params, batched, statsChan)
	}()
}
*/

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
