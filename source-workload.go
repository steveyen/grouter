package grouter

import (
	"log"
	"time"

	"github.com/dustin/gomemcached"
)

func WorkLoad(sourceSpec string, params Params, targetChan chan []Request,
	statsChan chan Stats) {
	run(sourceSpec, params.SourceMaxConns, targetChan, statsChan)
}

func run(sourceSpec string, sourceMaxConns int, targetChan chan []Request,
	statsChan chan Stats) {
	if sourceMaxConns > 1 {
		go run(sourceSpec, sourceMaxConns-1, targetChan, statsChan)
	}

	start := time.Now()
	report_every := 100000
	ops_per_round := 100
	ops := 0
	res := make(chan *gomemcached.MCResponse)
	for {
		reqs := make([]Request, ops_per_round)
		for i := range reqs {
			reqs[i] = Request{"default",
				&gomemcached.MCRequest{
					Opcode: gomemcached.GET,
					Key:    []byte("hello"),
				},
				res,
				uint32(sourceMaxConns),
			}
		}
		targetChan <- reqs
		for _ = range reqs {
			<-res
			ops++
		}

		if ops%report_every == 0 {
			now := time.Now()
			dur := now.Sub(start)
			log.Printf("ops/sec: %f", float64(report_every)/dur.Seconds())
			start = now
		}
	}
}
