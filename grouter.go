package main

import (
	"bufio"
	"bytes"
	"flag"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/dustin/gomemcached"
)

const (
	MARKER = gomemcached.CommandCode(0xff)
)

type Request struct {
	req *gomemcached.MCRequest
	res chan *gomemcached.MCResponse
}

type Source interface {
	Run(io.ReadWriter, chan Request)
}

func AcceptConns(ls net.Listener, maxConns int, source Source, target chan Request) {
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
					source.Run(s, target)
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

// ---------------------------------------------------------

var (
	crlf    = []byte("\r\n")
	version = []byte("VERSION grouter 0.0.0\r\n")

	resultClientErrorPrefix = []byte("CLIENT_ERROR ")
	resultServerErrorPrefix = []byte("SERVER_ERROR ")
)

type AsciiSource struct {
}

func (self AsciiSource) Run(s io.ReadWriter, target chan Request) {
	br := bufio.NewReader(s)
	bw := bufio.NewWriter(s)
	res := make(chan *gomemcached.MCResponse)

	for {
		buf, isPrefix, e := br.ReadLine()
		if e != nil {
			log.Printf("AsciiSource error: %s", e)
			return
		}
		if isPrefix {
			log.Printf("AsciiSource request is too long")
			return
		}

		log.Printf("read: '%s'", buf)

		req := strings.Split(strings.TrimSpace(string(buf)), " ")
		if asciiCmd, ok := asciiCmds[req[0]]; ok {
			if !asciiCmd.handler(&self, target, res, asciiCmd, req, br, bw) {
				return
			}
		} else {
			client_error(bw, "unknown command - " + req[0])
		}
	}
}

func client_error(bw *bufio.Writer, msg string) bool {
	bw.Write(resultClientErrorPrefix)
	bw.Write([]byte(msg))
	bw.Flush()
	return true
}

type AsciiCmd struct {
	opcode gomemcached.CommandCode
    handler func(*AsciiSource, chan Request, chan *gomemcached.MCResponse,
		*AsciiCmd, []string, *bufio.Reader, *bufio.Writer) bool
}

var asciiCmds = map[string]*AsciiCmd{
	"quit": &AsciiCmd{
		gomemcached.QUIT,
		func(source *AsciiSource,
			target chan Request, res chan *gomemcached.MCResponse,
			cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer) bool {
			return false
		},
	},
	"version": &AsciiCmd{
		gomemcached.VERSION,
		func(source *AsciiSource,
			target chan Request, res chan *gomemcached.MCResponse,
			cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer) bool {
			bw.Write([]byte(version))
			bw.Flush()
			return true
		},
	},
	"get": &AsciiCmd{
		gomemcached.GET,
		func(source *AsciiSource,
			target chan Request, res chan *gomemcached.MCResponse,
			cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer) bool {
			if len(req) != 2 {
				return client_error(bw, "expected 1 param for get command\r\n")
			}

			key := req[1]
			if len(key) <= 0 {
				return client_error(bw, "missing key\r\n")
			}

			log.Printf("get %s", key)

			bw.Write([]byte("END\r\n"))
			bw.Flush()
			return true
		},
	},
	"set":     &AsciiCmd{gomemcached.SET,     AsciiCmdMutation},
	"add":     &AsciiCmd{gomemcached.ADD,     AsciiCmdMutation},
	"replace": &AsciiCmd{gomemcached.REPLACE, AsciiCmdMutation},
	"prepend": &AsciiCmd{gomemcached.PREPEND, AsciiCmdMutation},
	"append":  &AsciiCmd{gomemcached.APPEND,  AsciiCmdMutation},
}

func AsciiCmdMutation(source *AsciiSource,
	target chan Request, res chan *gomemcached.MCResponse,
	cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer) bool {
	if len(req) != 5 {
		return client_error(bw, "expected 4 params for set command\r\n")
	}

	key := req[1]
	if len(key) <= 0 {
		return client_error(bw, "missing key\r\n")
	}

	flg, e := strconv.Atoi(req[2])
	if e != nil {
		return client_error(bw, "could not parse flag\r\n")
	}

	exp, e := strconv.Atoi(req[3])
	if e != nil {
		return client_error(bw, "could not parse expiration\r\n")
	}

	nval, e := strconv.Atoi(req[4])
	if e != nil || nval < 0 {
		return client_error(bw, "could not parse value length\r\n")
	}

	buf := make([]byte, nval + 2)

	nbuf, e := io.ReadFull(br, buf)
	if e != nil {
		log.Printf("AsciiSource error: %s", e)
		return false
	}
	if nbuf != nval + 2 {
		log.Printf("AsciiSource nbuf error: %s", e)
		return false
	}
	if !bytes.Equal(buf[nbuf - 2:], crlf) {
		return client_error(bw, "was expecting CRLF value termination\r\n")
	}
	val := buf[:nval]

	log.Printf("set %s %d %d %d = '%s'", key, flg, exp, nval, val)

	bw.Write([]byte("STORED\r\n"))
	bw.Flush()
	return true
}

// ---------------------------------------------------------

func MemoryTargetRun(incoming chan Request) {
	for {
		req := <-incoming
		log.Printf("mtr req: %s", req)
		res := &gomemcached.MCResponse{}
		req.res <-res
	}
}

// ---------------------------------------------------------

func MainServer(listen string, maxConns int) {
	ls, e := net.Listen("tcp", listen)
	if e != nil {
		log.Fatalf("error: could not listen on: %s; error: %s", listen, e)
	} else {
		defer ls.Close()
		log.Printf("listening to: %s", listen)

		memoryChanSize := 5
		memoryChanRequest := make(chan Request, memoryChanSize)
		go func() {
			MemoryTargetRun(memoryChanRequest)
		}()
		AcceptConns(ls, maxConns, &AsciiSource{}, memoryChanRequest)
	}
}

func main() {
	var listen *string = flag.String("listen", ":11300",
		"local address to listen to")
	var maxConns *int = flag.Int("max-conns", 3,
		"max conns allowed from clients")
	flag.Parse()
	MainServer(*listen, *maxConns)
}

