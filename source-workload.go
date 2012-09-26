package grouter
import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dustin/gomemcached"
)

type WorkLoadCfg struct {
	// Key-value map (see workload.json).
	cfg map[string]interface{}

	// Command generation decision tree (see workload-tree.json).
	cmd_tree []interface{}
}

// The source entry function for synthetic workload generation.
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

// Reads a workload cfg (JSON) from a file and the associated command
// generation decision tree.
func WorkLoadCfgRead(cfg_path string) WorkLoadCfg {
	cfg := ReadJSONFile(cfg_path).(map[string]interface{})
	if cfg["cmd-tree"] == nil {
		log.Fatalf("error: missing decision 'cmd-tree' attribute from: %v", cfg_path)
	}
	return WorkLoadCfg{
		cfg:      cfg,
		cmd_tree: ReadJSONFile(cfg["cmd-tree"].(string)).([]interface{}),
	}
}

// Logs a workload cfg for debugging/diagnosis.
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

// Returns an int from a workload cfg by key.
func WorkLoadCfgGetInt(cfg WorkLoadCfg, key string, defaultVal int) int {
	if cfg.cfg[key] != nil {
		return int(cfg.cfg[key].(float64))
	}
	return defaultVal
}

// Main function that sends workload requests and processes responses.
func WorkLoad(cfg WorkLoadCfg, clientNum uint32, sourceSpec string, target Target,
	statsChan chan Stats) {
	report := 100
	bucket := "default"
	batch := WorkLoadCfgGetInt(cfg, "batch", 100)

	tot_workload_ops_nsecs := int64(0) // In nanoseconds.
	tot_workload_ops := 0

	res := make(chan *gomemcached.MCResponse, batch)
	res_map := make(map[uint32]*gomemcached.MCResponse) // Key is opaque uint32.
	reqs_gen := make(chan []Request)

	// A separate goroutine generates the next batch concurrently
	// while a current batch is in-flight.
	go WorkLoadBatchRun(cfg, clientNum, sourceSpec, bucket, batch, reqs_gen, res)

	for reqs := range reqs_gen {
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
		tot_workload_ops += batch
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

// Helper function that generates a batch of workload requests onto a
// reqs_gen channel.
func WorkLoadBatchRun(cfg WorkLoadCfg, clientNum uint32, sourceSpec string,
	bucket string, batch int, reqs_gen chan []Request,
	res chan *gomemcached.MCResponse) {
	cur := make(map[string] uint64)
	opaque := uint32(0)
	for {
		reqs := make([]Request, batch)
		for i := 0; i < batch; i++ {
			cmd := WorkLoadNextCmd(cfg, clientNum, cfg.cmd_tree, 0, cur)
			log.Printf("%v", cmd)
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
}

var WorkLoadCmds = make(map[string] func(cfg WorkLoadCfg, clientNum uint32,
	cmd_tree []interface{}, pos int,
	cur map[string] uint64) (int, string))

func init() {
	WorkLoadCmds["choose"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		name_true := cmd_tree[pos + 1].(string)
		name_false := cmd_tree[pos + 2].(string)
		block_true := cmd_tree[pos + 3].([]interface{})
		block_false := cmd_tree[pos + 4].([]interface{})
		cur_true := cur["tot-" + name_true]
		cur_false := cur["tot-" + name_false]
		cur_total := cur_true + cur_false
		ratio_true := cfg.cfg["ratio-" + name_true].(float64)
		if float64(cur_true) / float64(cur_total) < ratio_true {
			cur["tot-" + name_true] += uint64(1)
			return 5, WorkLoadNextCmd(cfg, clientNum, block_true, 0, cur)
		}
		cur["tot-" + name_false] += uint64(1)
		return 5, WorkLoadNextCmd(cfg, clientNum, block_false, 0, cur)
	}
	WorkLoadCmds["new"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		cur["key"] = 2
		return 1, "new"
	}
	WorkLoadCmds["hot"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		cur["key"] = 1
		return 1, "hot"
	}
	WorkLoadCmds["cold"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		cur["key"] = 0
		return 1, "cold"
	}
	WorkLoadCmds["miss"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		cur["key"] = math.MaxUint64
		return 1, "miss"
	}
	WorkLoadCmds["create"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		return 1, "create " + strconv.FormatUint(cur["key"], 10)
	}
	WorkLoadCmds["delete"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		return 1, "delete " + strconv.FormatUint(cur["key"], 10)
	}
	WorkLoadCmds["set"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		return 1, "set " + strconv.FormatUint(cur["key"], 10)
	}
	WorkLoadCmds["get"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string] uint64) (int, string) {
		return 1, "get " + strconv.FormatUint(cur["key"], 10)
	}
}

func WorkLoadNextCmd(cfg WorkLoadCfg, clientNum uint32, cmd_tree []interface{},
	pos int, cur map[string] uint64) string {
	rv := ""
	for pos < len(cmd_tree) {
		cmd := cmd_tree[pos].(string)
		cmd_func := WorkLoadCmds[cmd]
		if cmd_func == nil {
			log.Fatalf("error: unknown workload cmd: %v", cmd)
		}
		func_inc, func_rv := cmd_func(cfg, clientNum, cmd_tree, pos, cur)
		pos += func_inc
		rv = func_rv
	}
	return rv
}

// Helper function to read a JSON formatted data file.
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
