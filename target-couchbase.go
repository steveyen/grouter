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

	// TODO: Need to handle bucket disappearing/reappearing/rebalancing.
	buckets := make(map[string] *couchbase.Bucket)

	getBucket := func(bucketName string) (res *couchbase.Bucket) {
		if res = buckets[bucketName]; res == nil {
			if res, _ = pool.GetBucket(bucketName); res != nil {
				buckets[bucketName] = res
			}
		}
		return res
	}

	worker := func(serverCh chan []Request) {
		// All the requests have same bucket and server.
		for reqs := range serverCh {
			if len(reqs) < 1 {
				continue
			}

			if bucket := getBucket(reqs[0].Bucket); bucket != nil {
				for _, req := range reqs {
					bucket.Do(string(req.Req.Key), func(c *memcached.Client, v uint16) error {
						req.Req.VBucket = v
						return c.Transmit(req.Req)
					})
				}

				for _, req := range reqs {
					bucket.Do(string(req.Req.Key), func(c *memcached.Client, v uint16) error {
						res, err := c.Receive()
						if err != nil || res == nil {
							res = &gomemcached.MCResponse{
								Opcode: req.Req.Opcode,
								Status: gomemcached.EINVAL,
								Opaque: req.Req.Opaque,
							}
						}
						req.Res <- res
						return err
					})
				}
			}
		}
	}

	serverChs := make(map[int] chan []Request)

	sendBatchToServer := func(serverIndex int, reqs []Request) {
		// All the requests have same bucket and server index.
		serverCh := serverChs[serverIndex]
		if serverCh == nil {
			serverCh = make(chan []Request, 10)
			serverChs[serverIndex] = serverCh
			go worker(serverCh)
		} else {
			serverCh <- reqs
		}
	}

	go func() {
		getServerIndex := func(bucketName string, key []byte) int {
			b := getBucket(bucketName)
			if b != nil {
				vbid := b.VBHash(string(key))
				return b.VBucketServerMap.VBucketMap[vbid][0]
			}
			return -1
		}

		for reqs := range incoming {
			SortRequests(reqs, getServerIndex) // Sort requests by server index.

			startSvr := -1
			startReq := -1

			for i, currReq := range reqs {
				currSvr := getServerIndex(currReq.Bucket, currReq.Req.Key)

				if startReq >= 0 {
					if reqs[startReq].Bucket == currReq.Bucket &&
						startSvr == currSvr {
						continue
					}
					sendBatchToServer(startSvr, reqs[startReq : i])
				}

				startSvr = currSvr
				startReq = i
			}
			sendBatchToServer(startSvr, reqs[startReq : len(reqs)])
		}
	}()
}
