package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"flag"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/dustin/gomemcached"
)

type Request struct {
	Req *gomemcached.MCRequest
	Res chan *gomemcached.MCResponse
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
			if !asciiCmd.Handler(&self, target, res, asciiCmd, req, br, bw) {
				return
			}
		} else {
			AsciiClientError(bw, "unknown command - " + req[0])
		}
	}
}

func AsciiClientError(bw *bufio.Writer, msg string) bool {
	bw.Write(resultClientErrorPrefix)
	bw.Write([]byte(msg))
	bw.Flush()
	return true
}

type AsciiCmd struct {
	Opcode gomemcached.CommandCode
    Handler func(*AsciiSource, chan Request, chan *gomemcached.MCResponse,
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
				return AsciiClientError(bw, "expected 1 param for get command\r\n")
			}
			key := req[1]
			if len(key) <= 0 {
				return AsciiClientError(bw, "missing key\r\n")
			}
			target <-Request{
				&gomemcached.MCRequest{
					Opcode: cmd.Opcode,
					Key: []byte(key),
				},
				res,
			}
			response := <-res
			if response.Status == gomemcached.SUCCESS {
				bw.Write([]byte("VALUE "))
				bw.Write([]byte(response.Key))
				bw.Write([]byte(" "))
				bw.Write([]byte(strconv.FormatUint(uint64(binary.BigEndian.Uint32(response.Extras)), 10)))
				bw.Write([]byte(" "))
				bw.Write([]byte(strconv.FormatUint(uint64(len(response.Body)), 10)))
				bw.Write([]byte("\r\n"))
				bw.Write(response.Body)
				bw.Write([]byte("\r\n"))
			}
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
		return AsciiClientError(bw, "expected 4 params for set command\r\n")
	}
	key := req[1]
	if len(key) <= 0 {
		return AsciiClientError(bw, "missing key\r\n")
	}
	flg, e := strconv.ParseUint(req[2], 10, 0)
	if e != nil {
		return AsciiClientError(bw, "could not parse flag\r\n")
	}
	exp, e := strconv.ParseUint(req[3], 10, 0)
	if e != nil {
		return AsciiClientError(bw, "could not parse expiration\r\n")
	}
	nval, e := strconv.Atoi(req[4])
	if e != nil || nval < 0 {
		return AsciiClientError(bw, "could not parse value length\r\n")
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
		return AsciiClientError(bw, "was expecting CRLF value termination\r\n")
	}
	val := buf[:nval]

	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras, uint32(flg))
	binary.BigEndian.PutUint32(extras[4:], uint32(exp))

	target <-Request{
		&gomemcached.MCRequest{
			Opcode: cmd.Opcode,
			Key: []byte(key),
			Extras: extras,
			Body: val,
		},
		res,
	}
	response := <-res
	if response.Status == gomemcached.SUCCESS {
		bw.Write([]byte("STORED\r\n"))
		bw.Flush()
		return true
	}
	bw.Write([]byte("SERVER_ERROR\r\n"))
	bw.Flush()
	return true
}

// ---------------------------------------------------------

type MemoryStorage struct {
	data map[string]gomemcached.MCItem
	cas  uint64
}

type MemoryStorageHandler func(s *MemoryStorage, req Request)

var MemoryStorageHandlers = map[gomemcached.CommandCode]MemoryStorageHandler{
	gomemcached.GET: func(s *MemoryStorage, req Request) {
		ret := &gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Opaque: req.Req.Opaque,
			Key: req.Req.Key,
		}
		if item, ok := s.data[string(req.Req.Key)]; ok {
			ret.Status = gomemcached.SUCCESS
			ret.Extras = make([]byte, 4)
			binary.BigEndian.PutUint32(ret.Extras, item.Flags)
			ret.Cas = item.Cas
			ret.Body = item.Data
		} else {
			ret.Status = gomemcached.KEY_ENOENT
		}
		req.Res <- ret
	},
	gomemcached.SET: func(s *MemoryStorage, req Request) {
		s.cas += 1
		s.data[string(req.Req.Key)] = gomemcached.MCItem{
			Flags: binary.BigEndian.Uint32(req.Req.Extras),
			Expiration: binary.BigEndian.Uint32(req.Req.Extras[4:]),
			Cas: s.cas,
			Data: req.Req.Body,
		}
		req.Res <- &gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Status: gomemcached.SUCCESS,
			Opaque: req.Req.Opaque,
			Cas: s.cas,
			Key: req.Req.Key,
		}
	},
}

func MemoryStorageRun(incoming chan Request) {
	s := MemoryStorage{data: make(map[string]gomemcached.MCItem)}
	for {
		req := <-incoming
		log.Printf("mtr req: %v", req)
		if h, ok := MemoryStorageHandlers[req.Req.Opcode]; ok {
			h(&s, req)
		} else {
			req.Res <-&gomemcached.MCResponse{
				Opcode: req.Req.Opcode,
				Status: gomemcached.UNKNOWN_COMMAND,
				Opaque: req.Req.Opaque,
			}
		}
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
			MemoryStorageRun(memoryChanRequest)
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

