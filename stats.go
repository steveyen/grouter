package grouter

import (
	"log"
	"strings"
	"time"
)

type Stats struct {
	Keys []string
	Vals []int64
}

func StartStatsReporter(chanSize int) chan Stats {
	reportChan := time.Tick(2 * time.Second)
	statsChan := make(chan Stats, chanSize)
	curr := make(map[string]int64)
	prev := make(map[string]int64)
	i := 0

	go func() {
		for {
			select {
			case stats := <-statsChan:
				for i := range stats.Keys {
					curr[stats.Keys[i]] += stats.Vals[i]
				}
			case <-reportChan:
				full := i%10 == 0
				if StatsReport(curr, prev, full) && full {
					log.Printf("-------------")
				}
				for k, v := range curr {
					prev[k] = v
				}
				i++
			}
		}
	}()

	return statsChan
}

func StatsReport(curr map[string]int64, prev map[string]int64, full bool) bool {
	// Reports rates on paired stats that follow a naming convention
	// like xxx and xxx_usecs.  For example, tot_ops and tot_ops_usecs.
	emitted := false
	for k, v := range curr {
		if strings.HasSuffix(k, "_usecs") {
			continue
		}
		k_usecs := k + "_usecs"
		v_usecs := curr[k_usecs]
		if v_usecs > prev[k_usecs] {
			k_per_usec := float64(v-prev[k]) / float64(v_usecs-prev[k_usecs])
			if k_per_usec > 0 {
				if full {
					log.Printf(" %v: %v, per sec: %f",
						k, v, k_per_usec*1000000.0)
					emitted = true
				} else {
					log.Printf(" %v per sec: %f", k, k_per_usec*1000000.0)
					emitted = true
				}
				continue
			}
		}
		if full && v != prev[k] {
			log.Printf(" %v: %v", k, v)
			emitted = true
		}
	}
	return emitted
}
