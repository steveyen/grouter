package main

import (
	"flag"
	"log"
	"sort"
	"strings"

	"github.com/steveyen/grouter"
)

type Params struct {
	sourceSpec string
	sourceMaxConns int

	targetSpec string
	targetChanSize int
}

type EndPoint struct {
	Usage string
	Func func(string, int, chan []grouter.Request)
}

// Available sources of requests.
var Sources = map[string]EndPoint{
	"memcached": EndPoint{
		"memcached:LISTEN_INTERFACE:LISTEN_PORT",
		grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"memcached-ascii": EndPoint{
		"memcached-ascii:LISTEN_INTERFACE:LISTEN_PORT",
		grouter.MakeListenSourceFunc(&grouter.AsciiSource{}),
	},
	"workload": EndPoint{"workload", grouter.WorkLoad},
}

// Available targets of requests.
var Targets = map[string]EndPoint{
	"http": EndPoint{
		"http:\\\\COUCHBASE_HOST:COUCHBASE_PORT",
        grouter.CouchbaseTargetRun,
	},
	"couchbase": EndPoint{
		"couchbase:\\\\COUCHBASE_HOST:COUCHBASE_PORT",
        grouter.CouchbaseTargetRun,
	},
	"memcached-ascii": EndPoint{
		"memcached-ascii:HOST:PORT",
		grouter.MemcachedAsciiTargetRun,
	},
	"memcached-binary": EndPoint{
		"memcached-binary:HOST:PORT",
		grouter.MemcachedBinaryTargetRun,
	},
	"memory": EndPoint{"memory", grouter.MemoryStorageRun},
}

func MainStart(params Params) {
	log.Printf("grouter")
	log.Printf("  source: %v", params.sourceSpec)
	log.Printf("    sourceMaxConns: %v", params.sourceMaxConns)
	log.Printf("  target: %v", params.targetSpec)
	log.Printf("    targetChanSize: %v", params.targetChanSize)

	sourceKind := strings.Split(params.sourceSpec, ":")[0]
	if source, ok := Sources[sourceKind]; ok {
		targetKind := strings.Split(params.targetSpec, ":")[0]
		if target, ok := Targets[targetKind]; ok {
			targetChan := make(chan []grouter.Request, params.targetChanSize)
			go func() {
				target.Func(params.targetSpec, params.targetChanSize, targetChan)
			}()
			source.Func(params.sourceSpec, params.sourceMaxConns, targetChan)
		} else {
			log.Fatalf("error: unknown target kind: %s", params.targetSpec)
		}
	} else {
		log.Fatalf("error: unknown source kind: %s", params.sourceSpec)
	}
}

func main() {
	sourceSpec := flag.String("source", "memcached-ascii::11300",
		"source of requests\n" +
		"    which should follow a format of KIND[:PARAMS] like..." +
		EndPointExamples(Sources))
	sourceMaxConns := flag.Int("source-max-conns", 3,
		"max conns allowed from source")

	targetSpec := flag.String("target", "memory",
		"target of requests\n" +
		"    which should follow a format of KIND[:PARAMS] like..." +
		EndPointExamples(Targets))
	targetChanSize := flag.Int("target-chan-size", 5,
		"target chan size to control concurrency")

	flag.Parse()
	MainStart(Params{
		sourceSpec:     *sourceSpec,
		sourceMaxConns: *sourceMaxConns,
		targetSpec:     *targetSpec,
		targetChanSize: *targetChanSize,
	})
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
