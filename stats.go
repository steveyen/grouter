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

	go func() {
		for {
			select {
			case stats := <-statsChan:
				for i := range stats.Keys {
					curr[stats.Keys[i]] += stats.Vals[i]
				}
			case <-reportChan:
				StatsReport(curr, prev)
				for k, v := range curr {
					prev[k] = v
				}
			}
		}
	}()

	return statsChan
}

func StatsReport(curr map[string]int64, prev map[string]int64) {
	// Reports rates on paired stats that follow a naming convention
	// like xxx and xxx_usecs.  For example, tot_ops and tot_ops_usecs.
	for k, v := range curr {
		if strings.HasSuffix(k, "_usecs") {
			continue
		}
		k_usecs := k + "_usecs"
		v_usecs := curr[k_usecs]
		if v_usecs > 0 {
			k_per_usec := float64(v-prev[k]) / float64(v_usecs-prev[k_usecs])
			if k_per_usec > 0 {
				log.Printf(" %v: %v (+%v) -- per sec: %f",
					k, v, v-prev[k], k_per_usec*1000000.0)
				continue
			}
		}
		log.Printf(" %v: %v (+%v)", k, v, v-prev[k])
	}
}
