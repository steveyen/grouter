package grouter

import (
	"log"
	"time"

	"github.com/dustin/gomemcached"
)

func WorkLoad(sourceSpec string, params Params, targetChan chan []Request) {
	workLoad(sourceSpec, params.SourceMaxConns, targetChan)
}

func workLoad(sourceSpec string, sourceMaxConns int, targetChan chan []Request) {
	if sourceMaxConns > 1 {
		go workLoad(sourceSpec, sourceMaxConns-1, targetChan)
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
