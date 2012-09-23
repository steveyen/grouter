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
	StartSource func(string, grouter.Params, []chan []grouter.Request, chan grouter.Stats)
	StartTarget func(string, grouter.Params, chan []grouter.Request, chan grouter.Stats)
	MaxConcurrency int // Some end-points have limited concurrency.
}

// Available sources of requests.
var Sources = map[string]EndPoint{
	"memcached": EndPoint{
		Usage: "memcached:LISTEN_INTERFACE:LISTEN_PORT",
		StartSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"memcached-ascii": EndPoint{
		Usage: "memcached-ascii:LISTEN_INTERFACE:LISTEN_PORT",
		StartSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"workload": EndPoint{
		Usage: "workload",
		StartSource: grouter.WorkLoad,
	},
}

// Available targets of requests.
var Targets = map[string]EndPoint{
	"http": EndPoint{
		Usage: "http://COUCHBASE_HOST:COUCHBASE_PORT",
        StartTarget: grouter.CouchbaseTargetRun,
	},
	"couchbase": EndPoint{
		Usage: "couchbase://COUCHBASE_HOST:COUCHBASE_PORT",
        StartTarget: grouter.CouchbaseTargetRun,
	},
	"memcached-ascii": EndPoint{
		Usage: "memcached-ascii:HOST:PORT",
		StartTarget: grouter.MemcachedAsciiTargetRun,
	},
	"memcached-binary": EndPoint{
		Usage: "memcached-binary:HOST:PORT",
		StartTarget: grouter.MemcachedBinaryTargetRun,
	},
	"memory": EndPoint{
		Usage: "memory",
		StartTarget: grouter.MemoryStorageRun,
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
	if source, ok := Sources[sourceKind]; ok {
		targetKind := strings.Split(params.TargetSpec, ":")[0]
		if target, ok := Targets[targetKind]; ok {
			if (target.MaxConcurrency > 0 &&
				target.MaxConcurrency < params.TargetConcurrency) {
				params.TargetConcurrency = target.MaxConcurrency
				log.Printf("    target-concurrency clipped to: %v;" +
					" due to limitations of target kind: %v",
					params.TargetConcurrency, targetKind)
			}

			statsChan := grouter.StartStatsReporter(
				params.SourceMaxConns + params.TargetConcurrency)

			targetChans := make([]chan []grouter.Request, params.TargetConcurrency)
			for i := range(targetChans) {
				targetChans[i] = make(chan []grouter.Request, params.TargetChanSize)
				StartTarget(target, targetChans[i], params, statsChan)
			}

			source.StartSource(params.SourceSpec, params, targetChans, statsChan)
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
		target.StartTarget(params.TargetSpec, params, batched, statsChan)
	}()
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
