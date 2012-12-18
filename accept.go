package grouter

import (
	"io"
	"log"
	"net"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
)

type Params struct {
	SourceSpec     string
	SourceMaxConns int

	TargetSpec        string
	TargetChanSize    int
	TargetConcurrency int
}

type Request struct {
	Bucket string
	Req    *gomemcached.MCRequest
	Res    chan *gomemcached.MCResponse

	// The client number allows backend targets to provide resource
	// affinity, such as processing requests using the same connection
	// used for a client's previous requests.  This also ensures
	// correct semantic ordering from the client's perspective.
	ClientNum uint32
}

type Target interface {
	PickChannel(clientNum uint32, bucket string) chan []Request
}

type Source interface {
	Run(s io.ReadWriter, clientNum uint32, target Target, statsChan chan Stats)
}

// Returns a source func that net.Listen()'s and accepts conns.
func MakeListenSourceFunc(source Source) func(string, Params, Target, chan Stats) {
	return func(sourceSpec string, params Params, target Target, statsChan chan Stats) {
		sourceParts := strings.Split(sourceSpec, ":")
		if len(sourceParts) == 3 {
			listen := strings.Join(sourceParts[1:], ":")
			ls, e := net.Listen("tcp", listen)
			if e != nil {
				log.Fatalf("error: could not listen on: %s; error: %s", listen, e)
			} else {
				defer ls.Close()
				log.Printf("listening to: %s", listen)
				AcceptConns(ls, params.SourceMaxConns, source, target, statsChan)
			}
		} else {
			log.Fatalf("error: missing listen HOST:PORT; instead, got: %v",
				strings.Join(sourceParts[1:], ":"))
		}
	}
}

// Accepts a max number of concurrent net.Conn's, starting a new
// goroutine for each accepted net.Conn.
func AcceptConns(ls net.Listener, maxConns int,
	source Source, target Target, statsChan chan Stats) {
	log.Printf("AcceptConns: accepting max conns: %d", maxConns)

	chanAccepted := make(chan io.ReadWriteCloser)
	chanClosed := make(chan io.ReadWriteCloser)
	numConns := 0
	totConns := uint32(0)

	go func() {
		for {
			c, e := ls.Accept()
			if e != nil {
				log.Printf("error: net.Listener.Accept() failed: %s", e)
				close(chanAccepted)
				return
			}
			chanAccepted <- c
		}
	}()

	for {
		if numConns < maxConns {
			log.Printf("AcceptConns: accepted conns: %d", numConns)
			select {
			case c := <-chanAccepted:
				if c == nil {
					log.Printf("AcceptConns: error: can't accept more conns")
					return
				}

				log.Printf("AcceptConns: conn accepted")
				numConns++
				totConns++

				go func(s io.ReadWriteCloser) {
					source.Run(s, totConns, target, statsChan)
					chanClosed <- s
					s.Close()
				}(c)
			case <-chanClosed:
				log.Printf("AcceptConns: conn closed")
				numConns--
			}
		} else {
			log.Printf("AcceptConns: reached max conns: %d", numConns)
			<-chanClosed
			log.Printf("AcceptConns: conn closed")
			numConns--
		}
	}
}

// Provides a capped, exponential-backoff retry loop around a dialer.
func Reconnect(spec string, dialer func(string) (interface{}, error)) interface{} {
	sleep := 100 * time.Millisecond
	for {
		client, err := dialer(spec)
		if err != nil {
			if sleep > 2000*time.Millisecond {
				sleep = 2000 * time.Millisecond
			}
			log.Printf("warn: reconnect failed: %s;"+
				" sleeping (ms): %d; err: %v",
				spec, sleep/time.Millisecond, err)
			time.Sleep(sleep)
			sleep = sleep * 2
		} else {
			return client
		}
	}

	return nil // Unreachable.
}

// Batch up requests from the incoming channel to feed to the outgoing channel.
func BatchRequests(maxBatchSize int, incoming chan []Request, outgoing chan []Request,
	statsChan chan Stats) {
	batch := make([]Request, 0, maxBatchSize)
	tot_batch := int64(0)

	for {
		if len(batch) > 0 {
			if len(batch) >= cap(batch) {
				outgoing <- batch
				tot_batch += 1
				batch = make([]Request, 0, maxBatchSize)
			} else {
				select {
				case outgoing <- batch:
					tot_batch += 1
					batch = make([]Request, 0, maxBatchSize)
				case reqs := <-incoming:
					batch = append(batch, reqs...)
				}
			}
		} else {
			reqs := <-incoming
			batch = append(batch, reqs...)
		}

		if tot_batch%200 == 0 {
			statsChan <- Stats{
				Keys: []string{"tot-batch"},
				Vals: []int64{tot_batch},
			}
			tot_batch = int64(0)
		}
	}
}
