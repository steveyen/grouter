package grouter

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"time"

	"github.com/dustin/gomemcached"
)

func WorkLoadRun(sourceSpec string, params Params, target Target,
	statsChan chan Stats) {
	cfg, cfg_tree := WorkLoadCfg("./workload.json")
	log.Printf("%v", cfg)
	log.Printf("%v", cfg_tree)

	for i := 1; i < params.TargetConcurrency; i++ {
		go WorkLoad(uint32(i), sourceSpec, target, statsChan)
	}
	WorkLoad(uint32(0), sourceSpec, target, statsChan)
}

func WorkLoadCfg(cfg_path string) (map[string]interface{}, []interface{}) {
	cfg := ReadJSONFile(cfg_path).(map[string]interface{})
	if cfg["tree"] == nil {
		log.Fatalf("error: missing decision 'tree' attribute from: %v", cfg_path)
	}
	cfg_tree := ReadJSONFile(cfg["tree"].(string)).([]interface{})
	return cfg, cfg_tree
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

func WorkLoad(clientNum uint32, sourceSpec string, target Target,
	statsChan chan Stats) {
	bucket := "default"
	report_every := 1000
	ops_per_round := 100
	tot_workload_ops_nsecs := int64(0) // In nanoseconds.
	tot_workload_ops := 0
	res := make(chan *gomemcached.MCResponse)
	res_map := make(map[uint32]*gomemcached.MCResponse) // Key is opaque uint32.
	opaque := uint32(0)
	for {
		reqs := make([]Request, ops_per_round)
		opaque_start := opaque
		for i := 0; i < ops_per_round; i++ {
			reqs[i] = Request{
				Bucket: bucket,
				Req: &gomemcached.MCRequest{
					Opcode: gomemcached.GET,
					Opaque: opaque,
					Key:    []byte("hello"),
				},
				Res:       res,
				ClientNum: clientNum,
			}
			opaque++
		}
		reqs_start := time.Now()
		targetChan := target.PickChannel(clientNum, bucket)
		targetChan <- reqs
		for i := 0; i < ops_per_round; i++ {
			// The responses might be out of order, where we use the
			// opaque field to sequence the responses.  We have a
			// res_map to stash early responses until needed.
			res_opaque := opaque_start + uint32(i)
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
		if tot_workload_ops%report_every == 0 {
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
