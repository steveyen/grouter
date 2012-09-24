package grouter

import (
	"log"
	"strings"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type MemcachedBinaryTarget struct {
	spec          string
	incomingChans []chan []Request
}

func (s MemcachedBinaryTarget) PickChannel(clientNum uint32, bucket string) chan []Request {
	return s.incomingChans[clientNum%uint32(len(s.incomingChans))]
}

func MemcachedBinaryTargetStart(spec string, params Params,
	statsChan chan Stats) Target {
	spec = strings.Replace(spec, "memcached-binary:", "", 1)

	s := MemcachedBinaryTarget{
		spec:          spec,
		incomingChans: make([]chan []Request, params.TargetConcurrency),
	}

	for i := range s.incomingChans {
		s.incomingChans[i] = make(chan []Request, params.TargetChanSize)
		MemcachedBinaryTargetStartIncoming(s, s.incomingChans[i])
	}

	return s
}

func MemcachedBinaryTargetStartIncoming(s MemcachedBinaryTarget, incoming chan []Request) {
	client, err := memcached.Connect("tcp", s.spec)
	if err != nil {
		log.Fatalf("error: memcached-binary connect failed: %s; err: %v", s.spec, err)
	}

	go func() {
		for reqs := range incoming {
			for _, req := range reqs {
				log.Printf("sending.....: %s; err: %v", s.spec, err)
				res, err := client.Send(req.Req)
				log.Printf("sending.done: %s; err: %v", s.spec, err)
				if err != nil {
					req.Res <- &gomemcached.MCResponse{
						Opcode: req.Req.Opcode,
						Status: gomemcached.EINVAL,
						Opaque: req.Req.Opaque,
					}
					log.Printf("warn: memcached-binary closing conn; saw error: %v", err)
					client.Close()
					client = Reconnect(s.spec, func(spec string) (interface{}, error) {
						return memcached.Connect("tcp", spec)
					}).(*memcached.Client)
				} else {
					req.Res <- res
				}
			}
		}
	}()
}
