package grouter

import (
	"log"

	"github.com/dustin/gomemcached"
)

func WorkLoad(sourceSpec string, sourceMaxConns int, targetChan chan Request) {
	if sourceMaxConns > 1 {
		go WorkLoad(sourceSpec, sourceMaxConns - 1, targetChan)
	}

	i := 0
	res := make(chan *gomemcached.MCResponse)
	for {
		targetChan <-Request{
			&gomemcached.MCRequest{
				Opcode: gomemcached.GET,
				Key: []byte("hello"),
			},
			res,
		}
		<-res
		i++

		log.Printf("workload: %d - %d", sourceMaxConns, i)
	}
}

