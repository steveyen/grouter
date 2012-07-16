package grouter

import (
	"log"
	"time"

	"github.com/dustin/gomemcached"
)

func WorkLoad(sourceSpec string, sourceMaxConns int, targetChan chan []Request) {
	if sourceMaxConns > 1 {
		go WorkLoad(sourceSpec, sourceMaxConns - 1, targetChan)
	}

	start := time.Now()
	report := 100000

	i := 0
	res := make(chan *gomemcached.MCResponse)
	for {
		reqs := make([]Request, 1)
		reqs[0] = Request{
			"default",
			&gomemcached.MCRequest{
				Opcode: gomemcached.GET,
				Key: []byte("hello"),
			},
			res,
		}
		targetChan <-reqs
		<-res
		i++

		if i % report == 0 {
			now := time.Now()
			dur := now.Sub(start)
			log.Printf("ops/sec: %f", float64(report) / dur.Seconds())
			start = now
		}
	}
}

