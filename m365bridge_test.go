package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestM365FinishPayload(t *testing.T) {
	body := m365FinishPayload("m365/gpt-5.6-sol", "你好")
	var payload map[string]any
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["model"] != "m365/gpt-5.6-sol" {
		t.Fatalf("model not echoed: %v", payload["model"])
	}
	choices, _ := payload["choices"].([]any)
	if len(choices) != 1 {
		t.Fatalf("bad choices: %v", payload["choices"])
	}
	msg, _ := choices[0].(map[string]any)["message"].(map[string]any)
	if msg["content"] != "你好" {
		t.Fatalf("bad content: %v", msg)
	}
}

func TestM365WriteChunk(t *testing.T) {
	var buf bytes.Buffer
	if err := m365WriteChunk(&buf, 1, "m365/x", "hi", false); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"content":"hi"`) {
		t.Fatalf("missing content: %q", buf.String())
	}
	buf.Reset()
	if err := m365WriteChunk(&buf, 1, "m365/x", "", true); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(buf.String(), `"finish_reason":"stop"`) || !strings.Contains(buf.String(), "[DONE]") {
		t.Fatalf("bad finish chunk: %q", buf.String())
	}
}

func TestResolveUnifiedChainM365Member(t *testing.T) {
	g := &gateway{cfg: config{autoPools: map[string][]string{"auto": {"m365/gpt-5.6-sol", "opencode/big-pickle"}}}}
	req := upstreamRequest{body: []byte(`{"model":"auto","messages":[]}`)}
	chain := g.resolveUnifiedChain(req)
	if len(chain) != 2 || chain[0] != "m365/gpt-5.6-sol" {
		t.Fatalf("m365 member not expanded: %v", chain)
	}
}
