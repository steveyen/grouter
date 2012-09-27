package grouter

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
)

var (
	crnl    = []byte("\r\n")
	space   = []byte(" ")
	version = []byte("VERSION grouter 0.0.0\r\n")
)

type AsciiSource struct {
	// A source that handles memcached ascii protocol requests.
}

func (self AsciiSource) Run(s io.ReadWriter, clientNum uint32, target Target,
	statsChan chan Stats) {
	tot_source_ascii_ops_nsecs := int64(0)
	tot_source_ascii_ops := 0

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

		req := strings.Split(strings.TrimSpace(string(buf)), " ")

		reqs_start := time.Now()
		if asciiCmd, ok := asciiCmds[req[0]]; ok {
			if !asciiCmd.Handler(&self, target, res, asciiCmd, req, br, bw, clientNum) {
				return
			}
		} else {
			AsciiClientError(bw, "unknown command - "+req[0]+"\r\n")
		}
		reqs_end := time.Now()

		tot_source_ascii_ops_nsecs += reqs_end.Sub(reqs_start).Nanoseconds()
		tot_source_ascii_ops += 1

		if tot_source_ascii_ops%100 == 0 {
			statsChan <- Stats{
				Keys: []string{
					"tot-source-ascii-ops",
					"tot-source-ascii-ops-usecs",
				},
				Vals: []int64{
					int64(tot_source_ascii_ops),
					int64(tot_source_ascii_ops_nsecs / 1000),
				},
			}
			tot_source_ascii_ops_nsecs = 0
			tot_source_ascii_ops = 0
		}
	}
}

type AsciiCmd struct {
	Opcode  gomemcached.CommandCode
	Handler func(source *AsciiSource,
		target Target, res chan *gomemcached.MCResponse,
		cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer,
		clientNum uint32) bool
}

var asciiCmds = map[string]*AsciiCmd{
	"quit": &AsciiCmd{
		gomemcached.QUIT,
		func(source *AsciiSource,
			target Target, res chan *gomemcached.MCResponse,
			cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer,
			clientNum uint32) bool {
			return false
		},
	},
	"version": &AsciiCmd{
		gomemcached.VERSION,
		func(source *AsciiSource,
			target Target, res chan *gomemcached.MCResponse,
			cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer,
			clientNum uint32) bool {
			bw.Write([]byte(version))
			bw.Flush()
			return true
		},
	},
	"get": &AsciiCmd{
		gomemcached.GET,
		func(source *AsciiSource,
			target Target, res chan *gomemcached.MCResponse,
			cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer,
			clientNum uint32) bool {
			if len(req) != 2 {
				return AsciiClientError(bw, "expected 1 param for get command\r\n")
			}
			key := req[1]
			if len(key) <= 0 {
				return AsciiClientError(bw, "missing key\r\n")
			}
			reqs := make([]Request, 1)
			reqs[0] = Request{
				"default",
				&gomemcached.MCRequest{
					Opcode: cmd.Opcode,
					Key:    []byte(key),
				},
				res,
				clientNum,
			}
			targetChan := target.PickChannel(clientNum, "default")
			targetChan <- reqs
			response := <-res
			if response.Status == gomemcached.SUCCESS {
				flg := uint64(binary.BigEndian.Uint32(response.Extras))

				bw.Write([]byte("VALUE "))
				bw.Write(response.Key)
				bw.Write(space)
				bw.Write([]byte(strconv.FormatUint(flg, 10)))
				bw.Write(space)
				bw.Write([]byte(strconv.FormatUint(uint64(len(response.Body)), 10)))
				bw.Write(crnl)
				bw.Write(response.Body)
				bw.Write(crnl)
			}
			bw.Write([]byte("END\r\n"))
			bw.Flush()
			return true
		},
	},
	"delete": &AsciiCmd{
		gomemcached.DELETE,
		func(source *AsciiSource,
			target Target, res chan *gomemcached.MCResponse,
			cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer,
			clientNum uint32) bool {
			if len(req) != 2 {
				return AsciiClientError(bw, "expected 1 param for delete command\r\n")
			}
			key := req[1]
			if len(key) <= 0 {
				return AsciiClientError(bw, "missing key\r\n")
			}
			reqs := make([]Request, 1)
			reqs[0] = Request{
				"default",
				&gomemcached.MCRequest{
					Opcode: cmd.Opcode,
					Key:    []byte(key),
				},
				res,
				clientNum,
			}
			targetChan := target.PickChannel(clientNum, "default")
			targetChan <- reqs
			response := <-res
			if response.Status == gomemcached.SUCCESS {
				bw.Write([]byte("DELETED\r\n"))
			} else {
				bw.Write([]byte("NOT_FOUND\r\n"))
			}
			bw.Flush()
			return true
		},
	},
	"set":     &AsciiCmd{gomemcached.SET, AsciiCmdMutation},
	"add":     &AsciiCmd{gomemcached.ADD, AsciiCmdMutation},
	"replace": &AsciiCmd{gomemcached.REPLACE, AsciiCmdMutation},
	"prepend": &AsciiCmd{gomemcached.PREPEND, AsciiCmdMutation},
	"append":  &AsciiCmd{gomemcached.APPEND, AsciiCmdMutation},
}

func AsciiCmdMutation(source *AsciiSource,
	target Target, res chan *gomemcached.MCResponse,
	cmd *AsciiCmd, req []string, br *bufio.Reader, bw *bufio.Writer,
	clientNum uint32) bool {
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
	buf := make([]byte, nval+2)
	nbuf, e := io.ReadFull(br, buf)
	if e != nil {
		log.Printf("AsciiSource error: %s", e)
		return false
	}
	if nbuf != nval+2 {
		log.Printf("AsciiSource nbuf error: %s", e)
		return false
	}
	if !bytes.Equal(buf[nbuf-2:], crnl) {
		return AsciiClientError(bw, "was expecting CRNL value termination\r\n")
	}
	val := buf[:nval]

	extras := make([]byte, 8)
	binary.BigEndian.PutUint32(extras, uint32(flg))
	binary.BigEndian.PutUint32(extras[4:], uint32(exp))

	reqs := make([]Request, 1)
	reqs[0] = Request{
		"default",
		&gomemcached.MCRequest{
			Opcode: cmd.Opcode,
			Key:    []byte(key),
			Extras: extras,
			Body:   val,
		},
		res,
		clientNum,
	}
	targetChan := target.PickChannel(clientNum, "default")
	targetChan <- reqs
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

func AsciiClientError(bw *bufio.Writer, msg string) bool {
	bw.Write([]byte("CLIENT_ERROR "))
	bw.Write([]byte(msg))
	bw.Flush()
	return true
}
