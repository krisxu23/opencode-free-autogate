package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// Codex /v1/responses 的 function 工具是扁平形态（name 在顶层），
// 不得被「剔除缺 function.name 工具」的卫生规则误删——误删后果是
// Codex 的每一轮请求都变成无工具请求，模型无法执行任何操作。
func TestEnhanceRequestBodyKeepsCodexFlatTools(t *testing.T) {
	body := []byte(`{"model":"gpt-5.3-codex","stream":true,"tools":[` +
		`{"type":"function","name":"shell","description":"run shell","parameters":{"type":"object","properties":{}}},` +
		`{"type":"function","name":"apply_patch","description":"patch","parameters":{"type":"object","properties":{}}},` +
		`{"type":"web_search"}]}`)
	payload := parseJSONObject(body)
	enhanceRequestBodyPayload(payload, false)
	tools, ok := payload["tools"].([]any)
	if !ok || len(tools) != 3 {
		t.Fatalf("Codex 扁平工具被误删：%d/%d", len(tools), 3)
	}
}

// chat 形态的畸形工具（缺 function.name）仍应被剔除——那是该规则的本职。
func TestEnhanceRequestBodyDropsMalformedChatTool(t *testing.T) {
	body := []byte(`{"model":"m","tools":[` +
		`{"type":"function"},` +
		`{"type":"function","function":{"name":"ok","parameters":{}}}]}`)
	payload := parseJSONObject(body)
	enhanceRequestBodyPayload(payload, false)
	tools, _ := payload["tools"].([]any)
	if len(tools) != 1 {
		t.Fatalf("畸形工具应被剔除，保留 %d 个", len(tools))
	}
	tool, _ := tools[0].(map[string]any)
	fn, _ := tool["function"].(map[string]any)
	if name, _ := fn["name"].(string); name != "ok" {
		t.Fatalf("保留的不应是畸形工具: %v", tool)
	}
}

func TestEnhanceRequestBodyNoChange(t *testing.T) {
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)
	payload := parseJSONObject(body)
	if enhanceRequestBodyPayload(payload, false) {
		t.Fatal("干净请求不应报有改动")
	}
	raw, _ := json.Marshal(payload)
	if !strings.Contains(string(raw), `"role":"user"`) {
		t.Fatalf("内容不应被改写: %s", raw)
	}
}
