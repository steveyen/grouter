package grouter

import (
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

	report_every := 100000
	ops_per_round := 100
	tot_workload_ops_nsecs := int64(0) // In nanoseconds.
	tot_workload_ops := 0
	res := make(chan *gomemcached.MCResponse)
	for {
		reqs := make([]Request, ops_per_round)
		for i := 0; i < ops_per_round; i++ {
			reqs[i] = Request{"default",
				&gomemcached.MCRequest{
					Opcode: gomemcached.GET,
					Key:    []byte("hello"),
				},
				res,
				uint32(sourceMaxConns),
			}
		}
		reqs_start := time.Now()
		targetChan <- reqs
		for i := 0; i < ops_per_round; i++ {
			<-res
		}
		reqs_end := time.Now()

		tot_workload_ops_nsecs += reqs_end.Sub(reqs_start).Nanoseconds()
		tot_workload_ops += ops_per_round
		if tot_workload_ops%report_every == 0 {
			statsChan <- Stats{
				Time: reqs_end,
				Keys: []string{
					"tot_workload_ops",
					"tot_workload_ops_usecs",
				},
				Vals: []int64{
					int64(tot_workload_ops),
					int64(tot_workload_ops_nsecs / 1000),
				},
			}
			tot_workload_ops_nsecs = int64(0)
			tot_workload_ops = 0
		}
	}
}
