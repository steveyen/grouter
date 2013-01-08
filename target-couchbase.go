package grouter

import (
	"log"
	"strings"

	"github.com/couchbaselabs/go-couchbase"
	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/client"
)

type CouchbaseTarget struct {
	spec          string
	incomingChans []chan []Request
}

func (s CouchbaseTarget) PickChannel(clientNum uint32, bucket string) chan []Request {
	return s.incomingChans[clientNum%uint32(len(s.incomingChans))]
}

type CouchbaseTargetHandler func(req Request, bucket *couchbase.Bucket)

func CouchbaseTargetKeyHandler(req Request, bucket *couchbase.Bucket) {
	var err error
	var res *gomemcached.MCResponse
	err = bucket.Do(string(req.Req.Key), func(mc *memcached.Client, vb uint16) error {
		req.Req.VBucket = vb
		res, err = mc.Send(req.Req)
		return err
	})
	if res == nil {
		res = &gomemcached.MCResponse{
			Status: gomemcached.KEY_ENOENT,
			Opcode: req.Req.Opcode,
			Opaque: req.Req.Opaque,
			Key:    req.Req.Key,
		}
	}
	req.Res <- res
}

var CouchbaseTargetHandlers = map[gomemcached.CommandCode]CouchbaseTargetHandler{
	gomemcached.GET:    CouchbaseTargetKeyHandler,
	gomemcached.SET:    CouchbaseTargetKeyHandler,
	gomemcached.DELETE: CouchbaseTargetKeyHandler,
}

func CouchbaseTargetStart(spec string, params Params,
	statsChan chan Stats) Target {
	spec = strings.Replace(spec, "couchbase:", "http:", 1)

	s := CouchbaseTarget{
		spec:          spec,
		incomingChans: make([]chan []Request, params.TargetConcurrency),
	}

	for i := range s.incomingChans {
		s.incomingChans[i] = make(chan []Request, params.TargetChanSize)
		CouchbaseTargetStartIncoming(s, s.incomingChans[i])
	}

	return s
}

func CouchbaseTargetStartIncoming(s CouchbaseTarget, incoming chan []Request) {
	client, err := couchbase.Connect(s.spec)
	if err != nil {
		log.Fatalf("error: couchbase connect failed: %s; err: %v", s.spec, err)
	}

	pool, err := client.GetPool("default")
	if err != nil {
		log.Fatalf("error: no default pool; err: %v", err)
	}

	go func() {
		var err error
		var lastBucketName string
		var lastBucket, bucket *couchbase.Bucket

		for reqs := range incoming {
			for _, req := range reqs {
				if req.Bucket == lastBucketName {
					bucket = lastBucket
				} else {
					bucket, err = pool.GetBucket(req.Bucket)
				}
				if err != nil {
					log.Printf("warn: missing bucket: %s; err: %v", req.Bucket, err)
					req.Res <- &gomemcached.MCResponse{
						Opcode: req.Req.Opcode,
						Status: gomemcached.EINVAL,
						Opaque: req.Req.Opaque,
					}
				} else {
					lastBucketName = req.Bucket
					lastBucket = bucket

					if h, ok := CouchbaseTargetHandlers[req.Req.Opcode]; ok {
						h(req, bucket)
					} else {
						req.Res <- &gomemcached.MCResponse{
							Opcode: req.Req.Opcode,
							Status: gomemcached.UNKNOWN_COMMAND,
							Opaque: req.Req.Opaque,
						}
					}
				}
			}
		}
	}()
}
