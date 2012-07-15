package main

import (
	"flag"
	"log"
	"net"

	"github.com/steveyen/grouter"
)

func MainStart(listen string, maxConns int,
	targetSpec string, targetChanSize int) {
	log.Printf("grouter")
	log.Printf("  listen: %v", listen)
	log.Printf("    maxConns: %v", maxConns)
	log.Printf("  target: %v", targetSpec)
	log.Printf("    targetChanSize: %v", targetChanSize)

	targetChan := make(chan grouter.Request, targetChanSize)
	var targetRun func(chan grouter.Request)

	if targetSpec == "memory:" {
		targetRun = grouter.MemoryStorageRun
	} else {
		log.Fatalf("error: unknown target spec: %s", targetSpec)
	}

	go func() {
		targetRun(targetChan)
	}()

	Server(listen, maxConns, targetChan)
}

func Server(listen string, maxConns int, targetChan chan grouter.Request) {
	ls, e := net.Listen("tcp", listen)
	if e != nil {
		log.Fatalf("error: could not listen on: %s; error: %s", listen, e)
	} else {
		defer ls.Close()
		log.Printf("listening to: %s", listen)
		grouter.AcceptConns(ls, maxConns, &grouter.AsciiSource{}, targetChan)
	}
}

func main() {
	listen := flag.String("listen", ":11300",
		"local address (<optional address>:port) to listen to")
	maxConns := flag.Int("max-conns", 3,
		"max conns allowed from clients")
	targetSpec := flag.String("target", "memory:",
		"valid targets are memory:, memcached://HOST:PORT")
	targetChanSize := flag.Int("target-chan-size", 5,
		"target chan size")
	flag.Parse()
	MainStart(*listen, *maxConns, *targetSpec, *targetChanSize)
}
