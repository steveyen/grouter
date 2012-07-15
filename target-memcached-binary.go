package grouter

import (
	"log"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

func MemcachedBinaryTargetRun(spec string, incoming chan Request) {
	spec = strings.Replace(spec, "memcached-binary:", "", 1)

	client, err := memcached.Connect("tcp", spec)
	if err != nil {
		log.Fatalf("error: memcached-binary connect failed: %s; err: %v", spec, err)
	}

	for {
		req := <-incoming
		log.Printf("sending.....: %s; err: %v", spec, err)
		res, err := client.Send(req.Req)
		log.Printf("sending.done: %s; err: %v", spec, err)
		if err != nil {
			req.Res <-&gomemcached.MCResponse{
				Opcode: req.Req.Opcode,
				Status: gomemcached.EINVAL,
				Opaque: req.Req.Opaque,
			}

			client.Close()
			sleep := 100 * time.Millisecond
			for {
				client, err = memcached.Connect("tcp", spec)
				if err != nil {
					if sleep > 2000 * time.Millisecond {
						sleep = 2000 * time.Millisecond
					}
					log.Printf("warn: memcached-binary reconnect failed: %s;" +
						" sleeping (ms): %d; err: %v",
						spec, sleep / time.Millisecond, err)
					time.Sleep(sleep)
					sleep = sleep * 2
				} else {
					break
				}
			}
		} else {
			req.Res <-res
		}
	}
}

