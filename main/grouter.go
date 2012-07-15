package main

import (
	"flag"
	"log"
	"net"

	"github.com/steveyen/grouter"
)

func MainServer(listen string, maxConns int, target string, targetChanSize int) {
	log.Printf("grouter")
	log.Printf("  listen: %v", listen)
	log.Printf("    maxConns: %v", maxConns)
	log.Printf("  target: %v", target)
	log.Printf("    targetChanSize: %v", targetChanSize)

	ls, e := net.Listen("tcp", listen)
	if e != nil {
		log.Fatalf("error: could not listen on: %s; error: %s", listen, e)
	} else {
		defer ls.Close()
		log.Printf("listening to: %s", listen)

		memoryChanRequest := make(chan grouter.Request, targetChanSize)
		go func() {
			grouter.MemoryStorageRun(memoryChanRequest)
		}()
		grouter.AcceptConns(ls, maxConns, &grouter.AsciiSource{}, memoryChanRequest)
	}
}

func main() {
	listen := flag.String("listen", ":11300",
		"local address (<optional address>:port) to listen to")
	maxConns := flag.Int("max-conns", 3,
		"max conns allowed from clients")
	target := flag.String("target", "memory:",
		"valid targets are memory:, memcached://HOST:PORT")
	targetChanSize := flag.Int("target-chan-size", 5,
		"target chan size")
	flag.Parse()
	MainServer(*listen, *maxConns, *target, *targetChanSize)
}

