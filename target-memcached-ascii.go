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

type MemcachedAsciiTargetHandler func(*bufio.Reader, *bufio.Writer, Request) error

var MemcachedAsciiTargetHandlers = map[gomemcached.CommandCode]MemcachedAsciiTargetHandler{
	gomemcached.GET: func(br *bufio.Reader, bw *bufio.Writer, req Request) error {
		bw.Write([]byte("get "))
		bw.Write(req.Req.Key)
		bw.Write(crnl)
		bw.Flush()

		hadValue := false

		for {
			line, isPrefix, err := br.ReadLine()
			if err != nil {
				return err
			}
			if isPrefix {
				return fmt.Errorf("error: line is too long")
			}

			parts := strings.Split(string(line), " ")
			if parts[0] == "VALUE" {
				flg, err := strconv.Atoi(parts[2])
				if err != nil {
					return err
				}

				nval, err := strconv.Atoi(parts[3])
				if err != nil {
					return err
				}

				buf := make([]byte, nval + 2)
				nbuf, err := io.ReadFull(br, buf)
				if err != nil {
					return err
				}
				if nbuf != nval + 2 {
					return fmt.Errorf("error: nbuf mismatch: %d != %d", nbuf, nval + 2)
				}
				if !bytes.Equal(buf[nbuf - 2:], crnl) {
					return fmt.Errorf("error: was expecting crlf")
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

				hadValue = true
			} else {
				if !hadValue {
					req.Res <-&gomemcached.MCResponse{
						Opcode: req.Req.Opcode,
						Status: gomemcached.KEY_ENOENT,
						Opaque: req.Req.Opaque,
						Key: req.Req.Key,
					}
				}
				return nil
			}
		}

		return fmt.Errorf("error: unreachable was reached")
	},
}

func MemcachedAsciiTargetRun(spec string, incoming chan Request) {
	spec = strings.Replace(spec, "memcached-ascii:", "", 1)

	conn, err := net.Dial("tcp", spec)
	if err != nil {
		log.Fatalf("error: memcached-ascii connect failed: %s; err: %v", spec, err)
	}
	br := bufio.NewReader(conn)
	bw := bufio.NewWriter(conn)

	for {
		req := <-incoming
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

