package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	n      = maelstrom.NewNode()
	kv     = maelstrom.NewLinKV(n)
	skv    = maelstrom.NewSeqKV(n)
	parctx = context.Background()
)

func main() {
	n.Handle("send", handleSend)
	n.Handle("poll", handlePoll)
	n.Handle("commit_offsets", handleCommitOffsets)
	n.Handle("list_committed_offsets", handleListCommittedOffsets)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

func handleSend(msg maelstrom.Message) error {
	var body map[string]any
	respbody := make(map[string]any, 0)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	key := body["key"].(string)
	val := int(body["msg"].(float64))
	respbody["type"] = "send_ok"
	respbody["in_reply_to"] = body["msg_id"]
	var offset int
	for {
		raw, err := kv.Read(parctx, key)
		if err != nil {
			raw = nil
		}
		oldval, err := decodePairs(raw)
		if err != nil {
			oldval = [][2]int{}
		}
		var oldoffset int
		if len(oldval) > 0 {
			oldoffset = oldval[len(oldval)-1][0]
		} else {
			oldoffset = -1
		}
		offset = oldoffset + 1
		newval := append(oldval, [2]int{offset, val})
		if err := kv.CompareAndSwap(parctx, key, oldval, newval, true); err == nil {
			break
		}
	}
	respbody["offset"] = offset
	return n.Reply(msg, respbody)
}

func handlePoll(msg maelstrom.Message) error {
	var body map[string]any
	respbody := make(map[string]any, 0)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	respbody["type"] = "poll_ok"
	respbody["in_reply_to"] = body["msg_id"]
	rawOffsets := body["offsets"].(map[string]any)
	// Convert from map[string]any to map[string]int
	offsets := make(map[string]int, len(rawOffsets))
	for k, v := range rawOffsets {
		// JSON numbers come in as float64
		f, ok := v.(float64)
		if !ok {
			return fmt.Errorf("offset for %q is not a number", k)
		}
		offsets[k] = int(f)
	}
	reply_msgs := make(map[string][][2]int)
	for k, of := range offsets {
		raw, err := kv.Read(parctx, k)
		if err != nil {
			raw = nil
		}
		oldval, err := decodePairs(raw)
		if err != nil {
			return err
		}
		for _, pai := range oldval {
			if pai[0] >= of {
				reply_msgs[k] = append(reply_msgs[k], pai)
			}
		}
	}
	respbody["msgs"] = reply_msgs
	return n.Reply(msg, respbody)
}

func handleCommitOffsets(msg maelstrom.Message) error {
	var body map[string]any
	respbody := make(map[string]any, 0)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	respbody["type"] = "commit_offsets_ok"
	respbody["in_reply_to"] = body["msg_id"]
	rawOffsets := body["offsets"].(map[string]any)
	// Convert from map[string]any to map[string]int
	offsets := make(map[string]int, len(rawOffsets))
	for k, v := range rawOffsets {
		// JSON numbers come in as float64
		f, ok := v.(float64)
		if !ok {
			return fmt.Errorf("offset for %q is not a number", k)
		}
		offsets[k] = int(f)
	}
	for k, of := range offsets {
		for {
			oldval, err := skv.ReadInt(parctx, k)
			if err != nil {
				oldval = -1
			}
			if of <= oldval {
				break
			}
			if err := skv.CompareAndSwap(parctx, k, oldval, of, true); err == nil {
				break
			}
		}
	}
	return n.Reply(msg, respbody)
}

func handleListCommittedOffsets(msg maelstrom.Message) error {
	var body map[string]any
	respbody := make(map[string]any, 0)
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	respbody["type"] = "list_committed_offsets_ok"
	respbody["in_reply_to"] = body["msg_id"]
	rawOffsets := body["keys"].([]any)
	// Convert from map[string]any to map[string]int
	offsets := make([]string, 0)
	for _, k := range rawOffsets {
		// JSON numbers come in as float64
		k1 := k.(string)
		offsets = append(offsets, k1)
	}
	reply_msgs := make(map[string]int)
	for _, k := range offsets {
		for {
			oldval, err := skv.ReadInt(parctx, k)
			if err != nil {
				oldval = -1
			}
			if oldval == -1 {
				break
			}
			reply_msgs[k] = oldval

		}
	}
	respbody["offsets"] = respbody
	return n.Reply(msg, respbody)
}

// decodePairs takes the raw interface{} from SeqKV and turns it
// into a Go slice of [2]int. If raw is nil, returns empty slice.
func decodePairs(raw any) ([][2]int, error) {
	if raw == nil {
		return [][2]int{}, nil
	}
	list, ok := raw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected type %T for kv value", raw)
	}
	var out [][2]int
	for _, elem := range list {
		arr, ok := elem.([]interface{})
		if !ok || len(arr) != 2 {
			// skip bad entries
			continue
		}
		xF, okX := arr[0].(float64)
		yF, okY := arr[1].(float64)
		if !okX || !okY {
			// skip non-numeric entries
			continue
		}
		out = append(out, [2]int{int(xF), int(yF)})
	}
	return out, nil
}

// TODO:Partition data across nodes to reduce raft rounds
