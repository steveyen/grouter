package grouter

import (
	"encoding/binary"
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"

	"github.com/dustin/gomemcached"
)

var (
	prefix_get = []byte("get ")
	prefix_set = []byte("set ")
	prefix_add = []byte("add ")
	prefix_replace = []byte("replace ")
	prefix_prepend = []byte("prepend ")
	prefix_append = []byte("append ")
)

type MemcachedAsciiTargetHandler func(*bufio.Reader, *bufio.Writer, Request) error

var MemcachedAsciiTargetHandlers = map[gomemcached.CommandCode]MemcachedAsciiTargetHandler{
	gomemcached.GET: func(br *bufio.Reader, bw *bufio.Writer, req Request) error {
		bw.Write(prefix_get)
		bw.Write(req.Req.Key)
		bw.Write(crnl)
		bw.Flush()

		numValues, endParts, err := AsciiTargetReadLines(br, req)
		if err != nil {
			return err
		}
		if endParts[0] == "END" {
			if numValues <= 0 {
				req.Res <-&gomemcached.MCResponse{
					Opcode: req.Req.Opcode,
					Status: gomemcached.KEY_ENOENT,
					Opaque: req.Req.Opaque,
					Key: req.Req.Key,
				}
			}
		} else {
			req.Res <-&gomemcached.MCResponse{
				Opcode: req.Req.Opcode,
				Status: gomemcached.EINVAL,
				Opaque: req.Req.Opaque,
				Key: req.Req.Key,
			}
		}
		return nil
	},
	gomemcached.SET: func(br *bufio.Reader, bw *bufio.Writer, req Request) error {
		return AsciiTargetMutation(br, bw, req, prefix_set)
	},
	gomemcached.ADD: func(br *bufio.Reader, bw *bufio.Writer, req Request) error {
		return AsciiTargetMutation(br, bw, req, prefix_add)
	},
	gomemcached.REPLACE: func(br *bufio.Reader, bw *bufio.Writer, req Request) error {
		return AsciiTargetMutation(br, bw, req, prefix_replace)
	},
	gomemcached.PREPEND: func(br *bufio.Reader, bw *bufio.Writer, req Request) error {
		return AsciiTargetMutation(br, bw, req, prefix_prepend)
	},
	gomemcached.APPEND: func(br *bufio.Reader, bw *bufio.Writer, req Request) error {
		return AsciiTargetMutation(br, bw, req, prefix_append)
	},
}

func AsciiTargetMutation(br *bufio.Reader, bw *bufio.Writer,
	req Request, cmd []byte) error {
	flg := uint64(binary.BigEndian.Uint32(req.Req.Extras))
	exp := uint64(binary.BigEndian.Uint32(req.Req.Extras[4:]))

	bw.Write(cmd)
	bw.Write(req.Req.Key)
	bw.Write(space)
	bw.Write([]byte(strconv.FormatUint(flg, 10)))
	bw.Write(space)
	bw.Write([]byte(strconv.FormatUint(exp, 10)))
	bw.Write(space)
	bw.Write([]byte(strconv.FormatUint(uint64(len(req.Req.Body)), 10)))
	bw.Write(crnl)
	bw.Write(req.Req.Body)
	bw.Write(crnl)
	bw.Flush()

	line, isPrefix, err := br.ReadLine()
	if err != nil {
		return err
	}
	if isPrefix {
		return fmt.Errorf("error: line is too long")
	}
	if string(line) == "STORED" {
		req.Res <-&gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Status: gomemcached.SUCCESS,
			Opaque: req.Req.Opaque,
			Key: req.Req.Key,
		}
	} else if string(line) == "NOT_STORED" {
		req.Res <-&gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Status: gomemcached.NOT_STORED,
			Opaque: req.Req.Opaque,
			Key: req.Req.Key,
		}
	} else {
		req.Res <-&gomemcached.MCResponse{
			Opcode: req.Req.Opcode,
			Status: gomemcached.EINVAL,
			Opaque: req.Req.Opaque,
			Key: req.Req.Key,
		}
	}
	return nil
}

func AsciiTargetReadLines(br *bufio.Reader, req Request) (int, []string, error) {
	numValues := 0

	for {
		line, isPrefix, err := br.ReadLine()
		if err != nil {
			return numValues, nil, err
		}
		if isPrefix {
			return numValues, nil, fmt.Errorf("error: line is too long")
		}

		parts := strings.Split(string(line), " ")
		if parts[0] == "VALUE" {
			flg, err := strconv.Atoi(parts[2])
			if err != nil {
				return numValues, parts, err
			}

			nval, err := strconv.Atoi(parts[3])
			if err != nil {
				return numValues, parts, err
			}

			buf := make([]byte, nval + 2)
			nbuf, err := io.ReadFull(br, buf)
			if err != nil {
				return numValues, parts, err
			}
			if nbuf != nval + 2 {
				err = fmt.Errorf("error: nbuf mismatch: %d != %d", nbuf, nval + 2)
				return numValues, parts, err
			}
			if !bytes.Equal(buf[nbuf - 2:], crnl) {
				err = fmt.Errorf("error: was expecting crlf")
				return numValues, parts, err
			}

			extras := make([]byte, 4)
			binary.BigEndian.PutUint32(extras, uint32(flg))

			req.Res <-&gomemcached.MCResponse{
				Opcode: req.Req.Opcode,
				Status: gomemcached.SUCCESS,
				Opaque: req.Req.Opaque,
				Extras: extras,
				Key: []byte(parts[1]),
				Body: buf[:nval],
			}

			numValues++
		} else {
			return numValues, parts, nil
		}
	}

	return numValues, nil, fmt.Errorf("error: unreachable was reached")
}

func MemcachedAsciiTargetRun(spec string, concurrency int, incoming chan []Request) {
	spec = strings.Replace(spec, "memcached-ascii:", "", 1)

	conn, err := net.Dial("tcp", spec)
	if err != nil {
		log.Fatalf("error: memcached-ascii connect failed: %s; err: %v", spec, err)
	}
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	for {
		reqs := <-incoming
		for _, req := range reqs {
			if h, ok := MemcachedAsciiTargetHandlers[req.Req.Opcode]; ok {
				err := h(br, bw, req)
				if err != nil {
					req.Res <-&gomemcached.MCResponse{
					Opcode: req.Req.Opcode,
						Status: gomemcached.EINVAL,
						Opaque: req.Req.Opaque,
					}

					log.Printf("warn: memcached-ascii closing conn; saw error: %v", err)
					conn.Close()
					conn = Reconnect(spec, func(spec string) (interface{}, error) {
						return net.Dial("tcp", spec)
					}).(net.Conn)
					br = bufio.NewReader(conn)
					bw = bufio.NewWriter(conn)
				}
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

