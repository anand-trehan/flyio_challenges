package main

import (
	"context"
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var (
	n      = maelstrom.NewNode()
	kv     = maelstrom.NewSeqKV(n)
	parctx = context.Background()
)

func main() {
	n.Handle("add", handleAdd)
	n.Handle("read", handleRead)

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

func handleAdd(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	val := int(body["delta"].(float64))
	body["type"] = "add_ok"
	body["in_reply_to"] = body["msg_id"]
	for {
		oldval, err := kv.ReadInt(parctx, n.ID())
		if err != nil {
			oldval = 0
		}
		if err := kv.CompareAndSwap(parctx, n.ID(), oldval, val+oldval, true); err == nil {
			break
		}
	}
	delete(body, "delta")
	return n.Reply(msg, body)
}

func handleRead(msg maelstrom.Message) error {
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	body["type"] = "read_ok"
	body["in_reply_to"] = body["msg_id"]
	sum := int(0)
	for _, nodeID := range n.NodeIDs() {
		oldval, err := kv.ReadInt(parctx, nodeID)
		if err != nil {
			oldval = 0
		}
		sum += oldval
	}
	body["value"] = sum
	return n.Reply(msg, body)
}
