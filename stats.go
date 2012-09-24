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
	statsChan := make(chan Stats, chanSize)

	go func() {
		reportSecs := 2 * time.Second
		reportChan := time.Tick(reportSecs)
		reportNum := 0
		curr := make(map[string]int64)
		prev := make(map[string]int64)
		for {
			select {
			case stats := <-statsChan:
				for i := range stats.Keys {
					curr[stats.Keys[i]] += stats.Vals[i]
				}
			case <-reportChan:
				full := reportNum%10 == 0
				if StatsReport(curr, prev, reportSecs, full) && full {
					log.Printf("-------------")
				}
				for k, v := range curr {
					prev[k] = v
				}
				reportNum++
			}
		}
	}()

	return statsChan
}

func StatsReport(curr map[string]int64, prev map[string]int64,
	reportSecs time.Duration, full bool) bool {
	// Reports rates on paired stats that follow a naming convention
	// like xxx and xxx_usecs.  For example, tot_ops and tot_ops_usecs.
	emitted := false
	for k, v := range curr {
		if strings.HasSuffix(k, "_usecs") {
			continue
		}
		if strings.HasPrefix(k, "tot_") {
			v_diff := v - prev[k]
			k_per_sec := float64(v_diff) / reportSecs.Seconds()
			if k_per_sec > 0 {
				if full {
					log.Printf("%v: %v, per sec: %f", k, v, k_per_sec)
				} else {
					k_usecs := k + "_usecs"
					d_usecs := float64(curr[k_usecs] - prev[k_usecs])
					if d_usecs > 0 {
						log.Printf("%v per sec: %f, avg latency: %f",
							k, k_per_sec, (d_usecs/1000000.0)/float64(v_diff))
					} else {
						log.Printf("%v per sec: %f", k, k_per_sec)
					}
				}
				emitted = true
				continue
			}
		}
		if full && v != prev[k] {
			log.Printf("%v: %v", k, v)
			emitted = true
		}
	}
	return emitted
}
