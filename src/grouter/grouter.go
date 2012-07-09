package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
)

type Pump interface {
	Run(br *bufio.Reader, bw *bufio.Writer) bool
}

func AcceptConns(ls net.Listener, maxConns int, pump Pump) {
	log.Printf("accepting max conns: %d", maxConns)

	chanAccepted := make(chan io.ReadWriteCloser)
	chanClosed := make(chan io.ReadWriteCloser)
	numConns := 0

	go func() {
		for {
			c, e := ls.Accept()
			if e != nil {
				log.Printf("error from net.Listener.Accept(): %s", e)
				close(chanAccepted)
				return
			}
			chanAccepted <- c
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
					defer func() {
						chanClosed <- s
						s.Close()
					}()

					br := bufio.NewReader(s)
					bw := bufio.NewWriter(s)

					for pump.Run(br, bw) {
					}
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

// ---------------------------------------------------------

var (
	crlf    = []byte("\r\n")
	version = []byte("VERSION grouter 0.0.0\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	resultServerErrorPrefix = []byte("SERVER_ERROR ")
)

type AsciiCmd func(AsciiPump, []string, *bufio.Reader, *bufio.Writer) bool

var asciiCmds = map[string]AsciiCmd{
	"quit": func(pump AsciiPump, req []string, br *bufio.Reader, bw *bufio.Writer) bool {
		return false
	},
	"version": func(pump AsciiPump, req []string, br *bufio.Reader, bw *bufio.Writer) bool {
		bw.Write([]byte(version))
		bw.Flush()
		return true
	},
}

type AsciiPump struct {
	whatever_state uint64
}

func (self AsciiPump) Run(br *bufio.Reader, bw *bufio.Writer) bool {
	buf, isPrefix, e := br.ReadLine()
	if e != nil {
		log.Printf("ProcessRequest error: %s", e)
		return false
	}
	if isPrefix {
		log.Printf("ProcessRequest request is too long")
		return false
	}

	log.Printf("read: '%s'", buf)

	req := strings.Split(strings.TrimSpace(string(buf)), " ")
	asciiCmd := asciiCmds[req[0]]
	if asciiCmd == nil {
		bw.Write(resultClientErrorPrefix)
		bw.Write([]byte("unknown command - "))
		bw.Write([]byte(req[0]))
		bw.Write(crlf)
		bw.Flush()
		return true
	}

	return asciiCmd(self, req, br, bw)
}

// ---------------------------------------------------------

func MainServer(port int, maxConns int) {
	ls, e := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if e != nil {
		log.Fatalf("error: could not listen on port: %d; error: %s", port, e)
		return
	}
	defer ls.Close()
	log.Printf("listening on port: %d", port)
	AcceptConns(ls, maxConns, &AsciiPump{})
}

func main() {
	var port *int = flag.Int("port", 11300, "port to listen to")
	var maxConns *int = flag.Int("max-conns", 3, "max conns allowed from clients")
	flag.Parse()
	MainServer(*port, *maxConns)
}

