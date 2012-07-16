package grouter

import (
	"log"
	"strings"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

func MemcachedBinaryTargetRun(spec string, params Params, incoming chan []Request) {
	spec = strings.Replace(spec, "memcached-binary:", "", 1)

	client, err := memcached.Connect("tcp", spec)
	if err != nil {
		log.Fatalf("error: memcached-binary connect failed: %s; err: %v", spec, err)
	}

	for {
		reqs := <-incoming
		for _, req := range reqs {
			log.Printf("sending.....: %s; err: %v", spec, err)
			res, err := client.Send(req.Req)
			log.Printf("sending.done: %s; err: %v", spec, err)
			if err != nil {
				req.Res <-&gomemcached.MCResponse{
					Opcode: req.Req.Opcode,
					Status: gomemcached.EINVAL,
					Opaque: req.Req.Opaque,
				}

				log.Printf("warn: memcached-binary closing conn; saw error: %v", err)
				client.Close()
				client = Reconnect(spec, func(spec string) (interface{}, error) {
					return memcached.Connect("tcp", spec)
				}).(*memcached.Client)
			} else {
				req.Res <-res
			}
		}
	}
}

