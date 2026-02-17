package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

var cnt int64

type util struct {
	neighbours    []string
	mu            sync.RWMutex
	allMsgs       map[float64]struct{}
	msgs          []float64
	neighbourAcks map[string]map[float64]struct{}
	topReady      sync.WaitGroup
}

type topology_body struct {
	Type     string              `json:"type"`
	Topo_map map[string][]string `json:"topology"`
	ID       float64             `json:"msg_id"`
}

var (
	n     = maelstrom.NewNode()
	utils = util{
		neighbours:    make([]string, 0),
		allMsgs:       make(map[float64]struct{}),
		msgs:          make([]float64, 0),
		neighbourAcks: make(map[string]map[float64]struct{}),
	}
)

func main() {
	utils.topReady.Add(1)
	n.Handle("echo", handleEcho)
	n.Handle("generate", handleGenerate)
	n.Handle("broadcast", handleBroadcast)
	n.Handle("topology", handleTopology)
	n.Handle("read", handleRead)
	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			periodic_batch_broadcast()
		}
	}()

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}

func (utils *util) learn_msg(str float64) bool {
	utils.mu.Lock()
	defer utils.mu.Unlock()

	// Check if message already exists
	if _, exists := utils.allMsgs[str]; exists {
		return false // Message already exists
	}

	// Add to both structures atomically
	// msgarr.msgs = append(msgarr.msgs, str)
	utils.allMsgs[str] = struct{}{}
	utils.msgs = append(utils.msgs, str)
	return true // New message added
}

func (utils *util) read_all() []float64 {
	utils.mu.RLock()
	defer utils.mu.RUnlock()
	copyOfItems := make([]float64, len(utils.msgs))
	copy(copyOfItems, utils.msgs)
	return copyOfItems
}

func handleEcho(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Update the message type to return back.
	body["type"] = "echo_ok"
	body["in_reply_to"] = body["msg_id"]

	// Echo the original message back with the updated message type.
	return n.Reply(msg, body)
}

func handleGenerate(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}

	// Update the message type to return back.
	body["type"] = "generate_ok"
	body["in_reply_to"] = body["msg_id"]
	cnt1 := atomic.AddInt64(&cnt, 1)
	// n.ID()
	id := fmt.Sprintf("%s-%d", n.ID(), cnt1)
	body["id"] = id
	// Echo the original message back with the updated message type.
	return n.Reply(msg, body)
}

func handleBroadcast(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	resp_body := make(map[string]any, 1)
	resp_body["type"] = "broadcast_ok"
	// if err := n.Reply(msg, resp_body); err != nil {
	// 	return err
	// }
	utils.topReady.Wait()
	if _, has := body["message"]; has {
		message_recv := body["message"].(float64)
		utils.learn_msg(message_recv)
		if err := n.Reply(msg, resp_body); err != nil {
			return err
		}
		return nil
	}

	if _, has := body["messages"]; has {
		messages_recv := body["messages"].([]any)
		for _, msg := range messages_recv {
			v := msg.(float64)
			utils.learn_msg(v)
		}
		if err := n.Reply(msg, resp_body); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func handleRead(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body map[string]any
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	// Update the message type to return back.
	resp_body := make(map[string]any, 3)
	resp_body["type"] = "read_ok"
	// store in message array
	resp_body["in_reply_to"] = body["msg_id"]
	resp_body["messages"] = utils.read_all()
	// Echo the original message back with the updated message type.
	return n.Reply(msg, resp_body)
}

func handleTopology(msg maelstrom.Message) error {
	// Unmarshal the message body as an loosely-typed map.
	var body topology_body
	if err := json.Unmarshal(msg.Body, &body); err != nil {
		return err
	}
	defer utils.topReady.Done()
	// Update the message type to return back.
	resp_body := make(map[string]any, 2)
	resp_body["type"] = "topology_ok"
	resp_body["in_reply_to"] = body.ID
	if err := n.Reply(msg, resp_body); err != nil {
		return err
	}
	utils.mu.Lock()
	defer utils.mu.Unlock()
	for _, id := range body.Topo_map[n.ID()] {
		if id == n.ID() {
			continue
		}
		utils.neighbours = append(utils.neighbours, id)
	}
	// Echo the original message back with the updated message type.
	return nil
}

func periodic_batch_broadcast() {
	allmsgset := utils.read_all()
	utils.mu.Lock()
	// For each neighbor, compute the delta
	for _, nb := range utils.neighbours {
		// ensure we have an ack‐set for this neighbor
		if _, ok := utils.neighbourAcks[nb]; !ok {
			utils.neighbourAcks[nb] = make(map[float64]struct{})
		}
		// build the delta list
		delta := make([]float64, 0)
		for _, m := range allmsgset {
			if _, got := utils.neighbourAcks[nb][m]; !got {
				delta = append(delta, m)
			}
		}
		if len(delta) == 0 {
			continue
		}

		// optimistically mark as “in‐flight” so we don't resend the same Δ
		for _, m := range delta {
			utils.neighbourAcks[nb][m] = struct{}{}
		}

		go broadcastToNeighbour(nb, delta)
	}
	utils.mu.Unlock()

}

func broadcastToNeighbour(nodeID string, data []float64) {
	if len(data) == 0 {
		return
	}
	broadcast_body := map[string]any{
		"type":     "broadcast",
		"messages": data,
	}

	// Retry with exponential backoff
	maxRetries := 3
	baseDelay := 50 * time.Millisecond

	for attempt := 0; attempt < maxRetries; attempt++ {
		err := n.RPC(nodeID, broadcast_body, func(msg maelstrom.Message) error {
			return nil
		})
		if err == nil {
			return // Success
		}

		//Exponential backoff
		delay := baseDelay * time.Duration(1<<uint(attempt))
		if delay > 5*time.Second {
			delay = 5 * time.Second
		}
		time.Sleep(delay)
	}
	utils.mu.Lock()
	for _, m := range data {
		delete(utils.neighbourAcks[nodeID], m)
	}

	// If we get here, all retries failed - log but don't crash
	//fmt.Printf("Failed to broadcast message to %s after %d retries\n", nodeID, maxRetries)
}
