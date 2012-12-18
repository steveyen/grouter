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
	Descrip string
	RunSource func(string, grouter.Params, grouter.Target, chan grouter.Stats)
	StartTarget func(string, grouter.Params, chan grouter.Stats) grouter.Target
	MaxConcurrency int // Some end-points have limited concurrency.
}

// Available sources of requests.
var Sources = map[string]EndPoint{
	"memcached": EndPoint{
		Usage: "memcached:LISTEN_INTERFACE:LISTEN_PORT",
		Descrip: "memcached ascii source",
		RunSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"memcached-ascii": EndPoint{
		Usage: "memcached-ascii:LISTEN_INTERFACE:LISTEN_PORT",
		Descrip: "memcached ascii source",
		RunSource: grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"workload": EndPoint{
		Usage: "workload",
		Descrip: "a simple workload generator",
		RunSource: grouter.WorkLoadRun,
	},
}

// Available targets of requests.
var Targets = map[string]EndPoint{
	"http": EndPoint{
		Usage: "http://COUCHBASE_HOST:COUCHBASE_PORT",
		Descrip: "couchbase server as a target",
        StartTarget: grouter.CouchbaseTargetStart,
	},
	"couchbase": EndPoint{
		Usage: "couchbase://COUCHBASE_HOST:COUCHBASE_PORT",
		Descrip: "couchbase server as a target",
        StartTarget: grouter.CouchbaseTargetStart,
	},
	"memcached-ascii": EndPoint{
		Usage: "memcached-ascii:HOST:PORT",
		Descrip: "memcached (ascii protocol) server as a target",
		StartTarget: grouter.MemcachedAsciiTargetStart,
	},
	"memcached-binary": EndPoint{
		Usage: "memcached-binary:HOST:PORT",
		Descrip: "memcached (binary protocol) server as a target",
		StartTarget: grouter.MemcachedBinaryTargetStart,
	},
	"memory": EndPoint{
		Usage: "memory",
		Descrip: "simple in-memory hashtable target",
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
		"max conns allowed into source")

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
		} else {
			log.Fatalf("error: unknown target kind: %s", params.TargetSpec)
		}
	} else {
		log.Fatalf("error: unknown source kind: %s", params.SourceSpec)
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
		rv = rv + "\n        -- " + m[s].Descrip
	}
	return rv
}
