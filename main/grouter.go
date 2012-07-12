package main

import (
	"flag"
	"log"
	"net"

	"github.com/steveyen/grouter"
)

func MainServer(listen string, maxConns int) {
	ls, e := net.Listen("tcp", listen)
	if e != nil {
		log.Fatalf("error: could not listen on: %s; error: %s", listen, e)
	} else {
		defer ls.Close()
		log.Printf("listening to: %s", listen)

		memoryChanSize := 5
		memoryChanRequest := make(chan grouter.Request, memoryChanSize)
		go func() {
			grouter.MemoryStorageRun(memoryChanRequest)
		}()
		grouter.AcceptConns(ls, maxConns, &grouter.AsciiSource{}, memoryChanRequest)
	}
}

func main() {
	var listen *string = flag.String("listen", ":11300",
		"local address (<optional address>:port) to listen to")
	var maxConns *int = flag.Int("max-conns", 3,
		"max conns allowed from clients")
	flag.Parse()
	MainServer(*listen, *maxConns)
}

