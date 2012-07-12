package grouter

import (
	"encoding/binary"
	"log"

	"github.com/dustin/gomemcached"
)

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

