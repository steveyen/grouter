package grouter

import (
	"log"
	"strings"

	"github.com/dustin/gomemcached"
	"github.com/couchbaselabs/go-couchbase"
)

type CouchbaseStorage struct {
	Pool couchbase.Pool
}

type CouchbaseStorageHandler func(s *CouchbaseStorage, req Request, bucket *couchbase.Bucket)

var CouchbaseStorageHandlers = map[gomemcached.CommandCode]CouchbaseStorageHandler{
	gomemcached.GET: func(s *CouchbaseStorage, req Request, bucket *couchbase.Bucket) {
		ret := &gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Opaque: req.Req.Opaque,
			Key: req.Req.Key,
		}
		ret.Status = gomemcached.KEY_ENOENT
		req.Res <- ret
	},
}

func CouchbaseStorageRun(spec string, incoming chan Request) {
	specHTTP := strings.Replace(spec, "couchbase:", "http:", 1)

	client, err := couchbase.Connect(specHTTP)
	if err != nil {
		log.Fatalf("error connecting to couchbase: %s; err: %v", specHTTP, err)
	}

	pool, err := client.GetPool("default")
	if err != nil {
		log.Fatalf("error getting default pool; err: %v", err)
	}

	s := CouchbaseStorage{Pool: pool}
	for {
		req := <-incoming
		bucket, err := pool.GetBucket(req.Bucket)
		if err != nil {
			log.Printf("warn: missing bucket: %s; err: %v", req.Bucket, err)
			req.Res <-&gomemcached.MCResponse{
				Opcode: req.Req.Opcode,
				Status: gomemcached.EINVAL,
				Opaque: req.Req.Opaque,
			}
		} else {
			if h, ok := CouchbaseStorageHandlers[req.Req.Opcode]; ok {
				h(&s, req, bucket)
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

