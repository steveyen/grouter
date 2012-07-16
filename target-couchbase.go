package grouter

import (
	"log"
	"strings"

	"github.com/dustin/gomemcached"
	"github.com/couchbaselabs/go-couchbase"
)

type CouchbaseTargetHandler func(req Request, bucket *couchbase.Bucket)

var CouchbaseTargetHandlers = map[gomemcached.CommandCode]CouchbaseTargetHandler{
	gomemcached.GET: func(req Request, bucket *couchbase.Bucket) {
		ret := &gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Opaque: req.Req.Opaque,
			Key: req.Req.Key,
		}
		ret.Status = gomemcached.KEY_ENOENT
		req.Res <- ret
	},
}

func CouchbaseTargetRun(spec string, concurrency int, incoming chan []Request) {
	specHTTP := strings.Replace(spec, "couchbase:", "http:", 1)

	client, err := couchbase.Connect(specHTTP)
	if err != nil {
		log.Fatalf("error: couchbase connect failed: %s; err: %v", specHTTP, err)
	}

	pool, err := client.GetPool("default")
	if err != nil {
		log.Fatalf("error: no default pool; err: %v", err)
	}

	for {
		reqs := <-incoming
		for _, req := range reqs {
			bucket, err := pool.GetBucket(req.Bucket)
			if err != nil {
				log.Printf("warn: missing bucket: %s; err: %v", req.Bucket, err)
				req.Res <-&gomemcached.MCResponse{
					Opcode: req.Req.Opcode,
					Status: gomemcached.EINVAL,
					Opaque: req.Req.Opaque,
				}
			} else {
			if h, ok := CouchbaseTargetHandlers[req.Req.Opcode]; ok {
					h(req, bucket)
				} else {
					req.Res <-&gomemcached.MCResponse{
						Opcode: req.Req.Opcode,
						Status: gomemcached.UNKNOWN_COMMAND,
						Opaque: req.Req.Opaque,
					}
				}
			}
		}
	}
}

