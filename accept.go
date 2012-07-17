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
	SourceSpec string
	SourceMaxConns int

	TargetSpec string
	TargetChanSize int
	TargetConcurrency int
}

type Request struct {
	Bucket string
	Req *gomemcached.MCRequest
	Res chan *gomemcached.MCResponse

	// The client number allows backend targets to provide resource
	// affinity, such as processing requests using the same connection
	// used for a client's previous requests.  This also ensures
	// correct semantic ordering from the client's perspective.
	ClientNum uint32
}

type Source interface {
	Run(s io.ReadWriter, clientNum uint32, targetChan chan []Request)
}

// Returns a source func that net.Listen()'s and accepts conns.
func MakeListenSourceFunc(source Source)func(string, Params, chan []Request) {
	return func(sourceSpec string, params Params, targetChan chan []Request) {
		sourceParts := strings.Split(sourceSpec, ":")
		if len(sourceParts) == 3 {
			listen := strings.Join(sourceParts[1:], ":")
			ls, e := net.Listen("tcp", listen)
			if e != nil {
				log.Fatalf("error: could not listen on: %s; error: %s", listen, e)
			} else {
				defer ls.Close()
				log.Printf("listening to: %s", listen)
				AcceptConns(ls, params.SourceMaxConns, source, targetChan)
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
	source Source, targetChan chan []Request) {
	log.Printf("accepting max conns: %d", maxConns)

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
			chanAccepted <-c
		}
	}()

	for {
		if numConns < maxConns {
			log.Printf("accepted conns: %d", numConns)
			select {
			case c := <-chanAccepted:
				if c == nil {
					log.Printf("error: can't accept more conns")
					return
				}

				log.Printf("conn accepted")
				numConns++
				totConns++

				go func(s io.ReadWriteCloser) {
					source.Run(s, totConns, targetChan)
					chanClosed <-s
					s.Close()
				}(c)
			case <-chanClosed:
				log.Printf("conn closed")
				numConns--
			}
		} else {
			log.Printf("reached max conns: %d", numConns)
			<-chanClosed
			log.Printf("conn closed")
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

// Batch up requests from the incoming channel to feed to the outgoing channel.
func BatchRequests(maxBatchSize int, incoming chan []Request, outgoing chan []Request) {
	batch := make([]Request, 0, maxBatchSize)

	for {
		if len(batch) > 0 {
			if len(batch) >= cap(batch) {
				outgoing <-batch
				batch = make([]Request, 0, maxBatchSize)
			} else {
				select {
				case outgoing <-batch:
					batch = make([]Request, 0, maxBatchSize)
				case reqs := <-incoming:
					batch = append(batch, reqs...)
				}
			}
		} else {
			reqs := <-incoming
			batch = append(batch, reqs...)
		}
	}
}

// Partition incoming requests into a lane based on client number affinity.
func PartitionRequests(incoming chan []Request, lanes []chan []Request) {
	for {
		reqs := <-incoming

		// TODO: assuming that all reqs in the slice have the same ClientNum.
		// TODO: one blocked lane can block all other lanes.
		lanes[reqs[0].ClientNum % uint32(len(lanes))] <-reqs
	}
}
