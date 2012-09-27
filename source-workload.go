package grouter

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"io"
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

const (
	DEFAULT_MAX_ITEM   = int64(10000)
	DEFAULT_MAX_CREATE = int64(10000)
	DEFAULT_RATIO_HOT  = float64(1.0)
	LARGE_PRIME        = uint64(9576890767)
)

// The source entry function for synthetic workload generation.
func WorkLoadRun(sourceSpec string, params Params, target Target,
	statsChan chan Stats) {
	cfg := WorkLoadCfgLog(WorkLoadCfgRead(sourceSpec, "./workload.json"))

	for i := 1; i < params.TargetConcurrency; i++ {
		go WorkLoad(cfg, uint32(i), sourceSpec, target, statsChan)
	}
	WorkLoad(cfg, uint32(0), sourceSpec, target, statsChan)
}

// Reads a workload cfg (JSON) from a file and the associated command
// generation decision tree.
func WorkLoadCfgRead(sourceSpec string, cfg_path string) WorkLoadCfg {
	if strings.HasPrefix(sourceSpec, "workload:") {
		sourceSpec = sourceSpec[len("workload:"):]
	}
	sourceSpecSplit := strings.Split(sourceSpec, ",")

	// The cfg-path value from the sourceSpec takes precedence.
	for _, kv := range sourceSpecSplit {
		kvArr := strings.Split(kv, "=")
		if len(kvArr) > 1 && kvArr[0] == "cfg-path" {
			cfg_path = kvArr[1]
		}
	}

	log.Printf("  cfg-path: %v", cfg_path)
	cfg := ReadJSONFile(cfg_path).(map[string]interface{})

	// The values from the sourceSpec take precedence.
	for _, kv := range sourceSpecSplit {
		kvArr := strings.Split(kv, "=")
		if len(kvArr) > 1 {
			cfg[kvArr[0]] = kvArr[1]
		}
	}

	if cfg["cmd-tree"] == nil {
		log.Fatalf("error: missing decision 'cmd-tree' parameter")
	}
	return WorkLoadCfg{
		cfg:      cfg,
		cmd_tree: ReadJSONFile(cfg["cmd-tree"].(string)).([]interface{}),
	}
}

// Logs a workload cfg for debugging/diagnosis.
func WorkLoadCfgLog(cfg WorkLoadCfg) WorkLoadCfg {
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
	return cfg
}

// Returns an int from a workload cfg by key.
func WorkLoadCfgGetFloat64(cfg WorkLoadCfg, key string, defaultVal float64) float64 {
	if cfg.cfg[key] != nil {
		return cfg.cfg[key].(float64)
	}
	return defaultVal
}

func WorkLoadCfgGetInt64(cfg WorkLoadCfg, key string, defaultVal int64) int64 {
	if cfg.cfg[key] != nil {
		return int64(cfg.cfg[key].(float64))
	}
	return defaultVal
}

func WorkLoadCfgGetInt(cfg WorkLoadCfg, key string, defaultVal int) int {
	if cfg.cfg[key] != nil {
		return int(cfg.cfg[key].(float64))
	}
	return defaultVal
}

func WorkLoadCfgGetString(cfg WorkLoadCfg, key string, defaultVal string) string {
	if cfg.cfg[key] != nil {
		return cfg.cfg[key].(string)
	}
	return defaultVal
}

// Main function that sends workload requests and processes responses.
func WorkLoad(cfg WorkLoadCfg, clientNum uint32, sourceSpec string, target Target,
	statsChan chan Stats) {
	report := 100
	bucket := "default"
	batch := WorkLoadCfgGetInt(cfg, "batch", 1000)

	tot_workload_ops_nsecs := int64(0) // In nanoseconds.
	tot_workload_ops := 0

	res := make(chan *gomemcached.MCResponse, batch)
	res_prev := make(map[uint32]*gomemcached.MCResponse) // Key is opaque uint32.
	reqs_gen := make(chan []Request)

	// A separate goroutine generates the next batch concurrently
	// while a current batch is in-flight.
	go WorkLoadBatchRun(cfg, clientNum, sourceSpec, bucket, batch,
		reqs_gen, res, statsChan)

	for reqs := range reqs_gen {
		reqs_start := time.Now()
		targetChan := target.PickChannel(clientNum, bucket)
		targetChan <- reqs
		for _, req := range reqs {
			// The responses might be out of order, where we use the
			// opaque field to sequence the responses.  We have a
			// res_prev to stash early responses until needed.
			res_opaque := req.Req.Opaque
			if res_prev[res_opaque] != nil {
				delete(res_prev, res_opaque)
			} else {
				mc_res := <-res
				if mc_res.Opaque != res_opaque {
					// TODO: assert(res_prev[res_opaque] == nil)
					res_prev[res_opaque] = mc_res
				}
			}
		}
		// TODO: assert(len(res_prev) == 0)
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
	res chan *gomemcached.MCResponse, statsChan chan Stats) {
	pre := make(map[string]uint64)
	cur := make(map[string]uint64)
	out := make([]gomemcached.MCRequest, batch)
	opaque := uint32(0)
	for {
		cur["out"] = 0
		for cur["out"] < uint64(len(out)) {
			WorkLoadNextCmd(cfg, clientNum, cfg.cmd_tree, 0, cur, out)
		}
		reqs := make([]Request, len(out))
		for i, mc_req := range out {
			reqs[i] = Request{
				Bucket: bucket,
				Req: &gomemcached.MCRequest{
					Opcode: mc_req.Opcode,
					Opaque: opaque,
					Key:    mc_req.Key,
					Extras: mc_req.Extras,
					Body:   mc_req.Body,
				},
				Res:       res,
				ClientNum: clientNum,
			}
			opaque++
		}
		reqs_gen <- reqs

		if opaque%100 == 0 {
			keys := make([]string, len(cur))
			vals := make([]int64, len(cur))
			i := 0
			for k, v := range cur {
				keys[i] = k
				vals[i] = int64(v - pre[k])
				i++
			}
			statsChan <- Stats{
				Keys: keys,
				Vals: vals,
			}
			for k, v := range cur {
				pre[k] = v
			}
		}
	}
}

// Runs the command decision tree once.  A single run might generate
// more than one request in the out array, depending on the user's
// decision tree, so we take care to stay under the out array size.
func WorkLoadNextCmd(cfg WorkLoadCfg, clientNum uint32, cmd_tree []interface{},
	pos int, cur map[string]uint64, out []gomemcached.MCRequest) string {
	rv := ""
	for pos < len(cmd_tree) && cur["out"] < uint64(len(out)) {
		cmd := cmd_tree[pos].(string)
		cmd_func := WorkLoadCmds[cmd]
		if cmd_func == nil {
			log.Fatalf("error: unknown workload cmd: %v", cmd)
		}
		pos += cmd_func(cfg, clientNum, cmd_tree, pos, cur, out)
	}
	return rv
}

// Table of commands that a command decision tree can use.
var WorkLoadCmds = make(map[string]func(cfg WorkLoadCfg, clientNum uint32,
	cmd_tree []interface{}, pos int,
	cur map[string]uint64, out []gomemcached.MCRequest) int)

func init() {
	// Evaluates ratios and recursively chooses either the left or
	// right block of commands.
	WorkLoadCmds["choose"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		var_left := cmd_tree[pos+1].(string)
		var_right := cmd_tree[pos+2].(string)
		block_left := cmd_tree[pos+3].([]interface{})
		block_right := cmd_tree[pos+4].([]interface{})
		cur_left := cur["tot-"+var_left]
		cur_right := cur["tot-"+var_right]
		cur_total := cur_left + cur_right
		ratio_left := cfg.cfg["ratio-"+var_left].(float64)
		if float64(cur_left)/float64(cur_total) < ratio_left {
			cur["tot-"+var_left] += uint64(1)
			WorkLoadNextCmd(cfg, clientNum, block_left, 0, cur, out)
			return 5
		}
		cur["tot-"+var_right] += uint64(1)
		WorkLoadNextCmd(cfg, clientNum, block_right, 0, cur, out)
		return 5
	}
	// Picks a new key.
	WorkLoadCmds["new"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		cur["key"] = cur["tot-item"]
		if cur["tot-item"] < uint64(WorkLoadCfgGetInt64(cfg,
			"max-item", DEFAULT_MAX_ITEM)) {
			if cur["tot-create"] < uint64(WorkLoadCfgGetInt64(cfg,
				"max-create", DEFAULT_MAX_CREATE)) {
				cur["tot-item"] += 1
			}
		}
		return 1
	}
	// Picks a hot key.
	WorkLoadCmds["hot"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		ratio_hot := WorkLoadCfgGetFloat64(cfg, "ratio-hot", DEFAULT_RATIO_HOT)
		items := uint64(float64(cur["tot-item"]) * ratio_hot)
		if items <= 0 {
			cur["key"] = cur["tot-item"] - 1
		} else {
			base := cur["tot-item"] - items
			cur["key"] = base + (cur["tot-ops"] % items)
		}
		return 1
	}
	// Picks a cold key.
	WorkLoadCmds["cold"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		ratio_hot := WorkLoadCfgGetFloat64(cfg, "ratio-hot", DEFAULT_RATIO_HOT)
		items := uint64(float64(cur["tot-item"]) * (1.0 - ratio_hot))
		if items <= 0 {
			cur["key"] = uint64(0)
		} else {
			base := uint64(0)
			cur["key"] = base + (cur["tot-ops"] % items)
		}
		return 1
	}
	// Picks a key that's not supposed to be in the db, so a miss.
	WorkLoadCmds["miss"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		cur["key"] = math.MaxUint64
		return 1
	}
	// Uses the current key for a SET.
	WorkLoadCmds["set"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		if cur["out"] < uint64(len(out)) {
			extras := make([]byte, 8)
			binary.BigEndian.PutUint32(extras, uint32(0)) // flg.
			binary.BigEndian.PutUint32(extras[4:], uint32(0)) // exp.
			out[cur["out"]] = gomemcached.MCRequest{
				Opcode: gomemcached.SET,
				Key:    []byte(WorkLoadKeyString(cfg, cur["key"])),
				Extras: extras,
				Body:   []byte(WorkLoadKeyString(cfg, cur["key"])),
			}
			cur["tot-ops-set"] += 1
			cur["tot-ops"] += 1
			cur["out"] += 1
		}
		return 1
	}
	// Uses the current key for a GET.
	WorkLoadCmds["get"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		if cur["out"] < uint64(len(out)) {
			out[cur["out"]] = gomemcached.MCRequest{
				Opcode: gomemcached.GET,
				Key:    []byte(WorkLoadKeyString(cfg, cur["key"])),
			}
			cur["tot-ops-get"] += 1
			cur["tot-ops"] += 1
			cur["out"] += 1
		}
		return 1
	}
	// Uses the current key for a DELETE.
	WorkLoadCmds["delete"] = func(cfg WorkLoadCfg, clientNum uint32,
		cmd_tree []interface{}, pos int,
		cur map[string]uint64, out []gomemcached.MCRequest) int {
		if cur["out"] < uint64(len(out)) {
			out[cur["out"]] = gomemcached.MCRequest{
				Opcode: gomemcached.DELETE,
				Key:    []byte(WorkLoadKeyString(cfg, cur["key"])),
			}
			cur["tot-ops-delete"] += 1
			cur["tot-ops"] += 1
			cur["out"] += 1
		}
		return 1
	}
}

func WorkLoadKeyString(cfg WorkLoadCfg, key uint64) string {
	s := strconv.FormatUint(key, 10)
	hashed := WorkLoadCfgGetInt(cfg, "hashed", 1) > 0
	if hashed {
		s = MD5(s)[0:16]
	}
	prefix := WorkLoadCfgGetString(cfg, "prefix", "")
	if len(prefix) > 0 {
		s = prefix + "-" + s
	}
	return s
}

func MD5(s string) string {
	h := md5.New()
	io.WriteString(h, s)
	return hex.EncodeToString(h.Sum(nil))
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
