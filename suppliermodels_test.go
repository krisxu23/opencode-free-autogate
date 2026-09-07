package main

import (
	"context"
	"testing"
)

func TestGenericModelsListed(t *testing.T) {
	g := &gateway{cfg: config{
		suppliers: []Supplier{{ID: "local-m365", Enabled: true, Models: []string{"gpt-5.6-sol"}}},
		autoPools: map[string][]string{"auto": {"local-m365/gpt-5.6-sol"}},
	}}
	ids := g.supplierModelIDs()
	found, foundAuto := false, false
	for _, id := range ids {
		if id == "local-m365/gpt-5.6-sol" {
			found = true
		}
		if id == "auto" {
			foundAuto = true
		}
	}
	if !found || !foundAuto {
		t.Fatalf("missing prefixed/auto models: %v", ids)
	}
}

func TestRewriteGenericPrefix(t *testing.T) {
	// 通用前缀在 rewrite 阶段必须保留：分发链路靠前缀识别供应商与
	// auto 池成员，真名剥离发生在 dispatchSupplier / dispatchM365。
	g := &gateway{cfg: config{suppliers: []Supplier{{ID: "local-m365", Enabled: true}}}}
	payload := map[string]any{"model": "local-m365/gpt-5.6-sol"}
	if g.rewriteModelPayload(context.Background(), payload) {
		t.Fatal("generic prefix must be preserved for dispatch")
	}
	if payload["model"] != "local-m365/gpt-5.6-sol" {
		t.Fatalf("prefix stripped too early: %v", payload["model"])
	}
}
