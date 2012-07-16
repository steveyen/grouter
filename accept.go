package grouter

import (
	"io"
	"log"
	"net"
	"time"

	"github.com/dustin/gomemcached"
)

type Request struct {
	Bucket string
	Req *gomemcached.MCRequest
	Res chan *gomemcached.MCResponse
}

type Source interface {
	Run(io.ReadWriter, chan []Request)
}

func AcceptConns(ls net.Listener, maxConns int, source Source, targetChan chan []Request) {
	log.Printf("accepting max conns: %d", maxConns)

	chanAccepted := make(chan io.ReadWriteCloser)
	chanClosed := make(chan io.ReadWriteCloser)
	numConns := 0

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

				go func(s io.ReadWriteCloser) {
					source.Run(s, targetChan)
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
