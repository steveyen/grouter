package grouter

import (
	"log"
	"time"

	"github.com/dustin/gomemcached"
)

const MAX_CONNS = 10

func WorkLoad(sourceSpec string, params Params, targetChan chan []Request) {
	run(sourceSpec, params.SourceMaxConns, targetChan)
}

func run(sourceSpec string, sourceMaxConns int, targetChan chan []Request) {
	if sourceMaxConns > 1 {
		if sourceMaxConns > MAX_CONNS {
			sourceMaxConns = MAX_CONNS
		}

		go run(sourceSpec, sourceMaxConns-1, targetChan)
	}

	start := time.Now()
	report := 100000

	i := 0
	res := make(chan *gomemcached.MCResponse)
	for {
		reqs := []Request{{
			"default",
			&gomemcached.MCRequest{
				Opcode: gomemcached.GET,
				Key:    []byte("hello"),
			},
			res,
			uint32(sourceMaxConns),
		}}
		targetChan <- reqs
		<-res
		i++

		if i%report == 0 {
			now := time.Now()
			dur := now.Sub(start)
			log.Printf("ops/sec: %f", float64(report)/dur.Seconds())
			start = now
		}
	}
}
