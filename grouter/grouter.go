package main

import (
	"flag"
	"log"
	"sort"
	"strings"

	"github.com/steveyen/grouter"
)

type endPoint struct {
	usage string // Help string.
	descrip string
	runSource func(string, grouter.Params, grouter.Target, chan grouter.Stats)
	startTarget func(string, grouter.Params, chan grouter.Stats) grouter.Target
	maxConcurrency int // Some end-points have limited concurrency.
}

// Available sources of requests.
var sources = map[string]endPoint{
	"memcached": endPoint{
		usage: "memcached:LISTEN_INTERFACE:LISTEN_PORT",
		descrip: "memcached ascii source",
		runSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"memcached-ascii": endPoint{
		usage: "memcached-ascii:LISTEN_INTERFACE:LISTEN_PORT",
		descrip: "memcached ascii source",
		runSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"workload": endPoint{
		usage: "workload",
		descrip: "a simple workload generator",
		runSource: grouter.WorkLoadRun,
	},
}

// Available targets of requests.
var targets = map[string]endPoint{
	"http": endPoint{
		usage: "http://COUCHBASE_HOST:COUCHBASE_PORT",
		descrip: "couchbase server as a target",
        startTarget: grouter.CouchbaseTargetStart,
	},
	"couchbase": endPoint{
		usage: "couchbase://COUCHBASE_HOST:COUCHBASE_PORT",
		descrip: "couchbase server as a target",
        startTarget: grouter.CouchbaseTargetStart,
	},
	"memcached-ascii": endPoint{
		usage: "memcached-ascii:HOST:PORT",
		descrip: "memcached (ascii protocol) server as a target",
		startTarget: grouter.MemcachedAsciiTargetStart,
	},
	"memcached-binary": endPoint{
		usage: "memcached-binary:HOST:PORT",
		descrip: "memcached (binary protocol) server as a target",
		startTarget: grouter.MemcachedBinaryTargetStart,
	},
	"memory": endPoint{
		usage: "memory",
		descrip: "simple in-memory hashtable target",
		startTarget: grouter.MemoryStorageStart,
		maxConcurrency: 1,
	},
}

func main() {
	sourceSpec := flag.String("source", "memcached-ascii::11300",
        "source of requests\n" +
        "    as SOURCE_KIND[:MORE_PARAMS]\n" +
        "    examples..." + endPointExamples(sources))
	sourceMaxConns := flag.Int("source-max-conns", 100,
		"max conns allowed via source")

	targetSpec := flag.String("target", "memory",
        "target of requests\n" +
        "    as TARGET_KIND[:MORE_PARAMS]\n" +
        "    examples..." + endPointExamples(targets))
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
	if sourceDef, ok := sources[sourceKind]; ok {
		targetKind := strings.Split(params.TargetSpec, ":")[0]
		if targetDef, ok := targets[targetKind]; ok {
			if (targetDef.maxConcurrency > 0 &&
				targetDef.maxConcurrency < params.TargetConcurrency) {
				params.TargetConcurrency = targetDef.maxConcurrency
				log.Printf("    target-concurrency clipped to: %v;" +
					" due to limitations of target kind: %v",
					params.TargetConcurrency, targetKind)
			}

			statsChan := grouter.StartStatsReporter(
				params.SourceMaxConns + params.TargetConcurrency)

			target := targetDef.startTarget(params.TargetSpec, params, statsChan)

			sourceDef.runSource(params.SourceSpec, params, target, statsChan)
		} else {
			log.Fatalf("error: unknown target kind: %s", params.TargetSpec)
		}
	} else {
		log.Fatalf("error: unknown source kind: %s", params.SourceSpec)
	}
}

func endPointExamples(m map[string]endPoint) (rv string) {
	mk := make([]string, len(m))
	i := 0
    for k, _ := range m {
		mk[i] = k
		i++
    }
    sort.Strings(mk)
	rv = ""
	for _, s := range mk {
		rv = rv + "\n      " + m[s].usage
		rv = rv + "\n        -- " + m[s].descrip
	}
	return rv
}
