package grouter

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
)

type WorkLoadCfg struct {
	cfg      map[string]interface{} // Key-value map (see workload.json).
	cfg_tree []interface{}          // Decision tree (see workload-tree.json).
}

func WorkLoadRun(sourceSpec string, params Params, target Target,
	statsChan chan Stats) {
	cfg_path := "./workload.json" // TODO: Get cfg_path from params.
	log.Printf("  cfg_path: %v", cfg_path)
	cfg := WorkLoadCfgRead(cfg_path)
	WorkLoadCfgLog(cfg)

	for i := 1; i < params.TargetConcurrency; i++ {
		go WorkLoad(cfg, uint32(i), sourceSpec, target, statsChan)
	}
	WorkLoad(cfg, uint32(0), sourceSpec, target, statsChan)
}

func WorkLoadCfgRead(cfg_path string) WorkLoadCfg {
	cfg := ReadJSONFile(cfg_path).(map[string]interface{})
	if cfg["tree"] == nil {
		log.Fatalf("error: missing decision 'tree' attribute from: %v", cfg_path)
	}
	return WorkLoadCfg{
		cfg:      cfg,
		cfg_tree: ReadJSONFile(cfg["tree"].(string)).([]interface{}),
	}
}

func WorkLoadCfgLog(cfg WorkLoadCfg) {
	keys := make([]string, 0, len(cfg.cfg))
	for key := range cfg.cfg {
		if !strings.HasSuffix(key, "-") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	for _, key := range keys {
		if !strings.HasSuffix(key, "-") {
			log.Printf("    %v: %v - %v", key, cfg.cfg[key], cfg.cfg[key+"-"])
		}
	}
}

func WorkLoadCfgGetInt(cfg WorkLoadCfg, key string, defaultVal int) int {
	if cfg.cfg[key] != nil {
		return int(cfg.cfg[key].(float64))
	}
	return defaultVal
}

func WorkLoad(cfg WorkLoadCfg, clientNum uint32, sourceSpec string, target Target,
	statsChan chan Stats) {
	bucket := "default"
	report := 100
	ops_per_round := WorkLoadCfgGetInt(cfg, "batch", 100)
	tot_workload_ops_nsecs := int64(0) // In nanoseconds.
	tot_workload_ops := 0
	reqs_gen := make(chan []Request)
	res_map := make(map[uint32]*gomemcached.MCResponse) // Key is opaque uint32.
	res := make(chan *gomemcached.MCResponse, ops_per_round)

	go func() {
		reqs := make([]Request, ops_per_round)
		opaque := uint32(0)
		for {
			for i := 0; i < ops_per_round; i++ {
				reqs[i] = Request{
					Bucket: bucket,
					Req: &gomemcached.MCRequest{
						Opcode: gomemcached.GET,
						Opaque: opaque,
						Key:    []byte(strconv.FormatInt(int64(i), 10)),
					},
					Res:       res,
					ClientNum: clientNum,
				}
				opaque++
			}
			reqs_gen <- reqs
		}
	}()

	for {
		reqs := <-reqs_gen
		reqs_start := time.Now()
		targetChan := target.PickChannel(clientNum, bucket)
		targetChan <- reqs
		for _, req := range reqs {
			// The responses might be out of order, where we use the
			// opaque field to sequence the responses.  We have a
			// res_map to stash early responses until needed.
			res_opaque := req.Req.Opaque
			if res_map[res_opaque] != nil {
				delete(res_map, res_opaque)
			} else {
				mc_res := <-res
				if mc_res.Opaque != res_opaque {
					// TODO: assert(res_map[res_opaque] == nil)
					res_map[res_opaque] = mc_res
				}
			}
		}
		// TODO: assert(len(res_map) == 0)
		reqs_end := time.Now()

		tot_workload_ops_nsecs += reqs_end.Sub(reqs_start).Nanoseconds()
		tot_workload_ops += ops_per_round
		if tot_workload_ops%report == 0 {
			statsChan <- Stats{
				Keys: []string{
					"tot_workload_ops",
					"tot_workload_ops_usecs",
				},
				Vals: []int64{
					int64(tot_workload_ops),
					int64(tot_workload_ops_nsecs / 1000),
				},
			}
			tot_workload_ops_nsecs = int64(0)
			tot_workload_ops = 0
		}
	}
}

func ReadJSONFile(path string) interface{} {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("error: could not read: %v; err: %v", path, err)
	}
	var data interface{}
	err = json.Unmarshal(bytes, &data)
	if err != nil {
		log.Fatalf("error: could not parse json from: %v; err: %v", path, err)
	}
	return data
}
