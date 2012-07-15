package main

import (
	"flag"
	"log"
	"net"
	"strings"

	"github.com/steveyen/grouter"
)

func NetListenSourceFunc(source grouter.Source)func(string, int, chan grouter.Request) {
	return func(sourceSpec string, sourceMaxConns int,
		targetChan chan grouter.Request) {
		sourceParts := strings.Split(sourceSpec, ":")
		if len(sourceParts) == 3 {
			listen := strings.Join(sourceParts[1:], ":")
			ls, e := net.Listen("tcp", listen)
			if e != nil {
				log.Fatalf("error: could not listen on: %s; error: %s", listen, e)
			} else {
				defer ls.Close()
				log.Printf("listening to: %s", listen)
				grouter.AcceptConns(ls, sourceMaxConns, source, targetChan)
			}
		} else {
			log.Fatalf("error: missing listen HOST:PORT; instead, got: %v",
				strings.Join(sourceParts[1:], ":"))
		}
	}
}

var sourceFuncs = map[string]func(string, int, chan grouter.Request){
	"memcached":       NetListenSourceFunc(&grouter.AsciiSource{}),
	"memcached-ascii": NetListenSourceFunc(&grouter.AsciiSource{}),
	"workload":        grouter.WorkLoad,
}

var targetFuncs = map[string]func(string, chan grouter.Request){
	"memory": grouter.MemoryStorageRun,
}

func MainStart(sourceSpec string, sourceMaxConns int,
	targetSpec string, targetChanSize int) {
	log.Printf("grouter")
	log.Printf("  source: %v", sourceSpec)
	log.Printf("    sourceMaxConns: %v", sourceMaxConns)
	log.Printf("  target: %v", targetSpec)
	log.Printf("    targetChanSize: %v", targetChanSize)

	sourceKind := strings.Split(sourceSpec, ":")[0]
	if sourceFunc, ok := sourceFuncs[sourceKind]; ok {
		targetKind := strings.Split(targetSpec, ":")[0]
		if targetFunc, ok := targetFuncs[targetKind]; ok {
			targetChan := make(chan grouter.Request, targetChanSize)
			go func() {
				targetFunc(targetSpec, targetChan)
			}()
			sourceFunc(sourceSpec, sourceMaxConns, targetChan)
		} else {
			log.Fatalf("error: unknown target kind: %s", targetSpec)
		}
	} else {
		log.Fatalf("error: unknown source kind: %s", sourceSpec)
	}
}

func main() {
	sourceSpec := flag.String("source", "memcached-ascii::11300",
		"source of requests\n" +
		"    in the format of KIND[:PARAMS] like...\n" +
		"      memcached-ascii:LISTEN_INTERFACE:LISTEN_PORT")
	sourceMaxConns := flag.Int("source-max-conns", 3,
		"max conns allowed from source")
	targetSpec := flag.String("target", "memory:",
		"target of requests\n" +
		"    in the format of KIND[:PARAMS] like...\n" +
		"      memory:\n" +
		"      memcached:HOST:PORT")
	targetChanSize := flag.Int("target-chan-size", 5,
		"target chan size to control concurrency")
	flag.Parse()
	MainStart(*sourceSpec, *sourceMaxConns, *targetSpec, *targetChanSize)
}
