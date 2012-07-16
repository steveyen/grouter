package grouter

import (
	"log"
	"time"
)

// Provides a capped, exponential-backoff retry loop around a dialer func.
func Reconnect(spec string, dialer func(string) (interface{}, error)) interface{} {
	sleep := 100 * time.Millisecond
	for {
		client, err := dialer(spec)
		if err != nil {
			if sleep > 2000 * time.Millisecond {
				sleep = 2000 * time.Millisecond
			}
			log.Printf("warn: reconnect failed: %s;" +
				" sleeping (ms): %d; err: %v",
				spec, sleep / time.Millisecond, err)
			time.Sleep(sleep)
			sleep = sleep * 2
		} else {
			return client
		}
	}

	return nil // Unreachable.
}
