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
	g := &gateway{cfg: config{suppliers: []Supplier{{ID: "local-m365", Enabled: true}}}}
	payload := map[string]any{"model": "local-m365/gpt-5.6-sol"}
	if !g.rewriteModelPayload(context.Background(), payload) {
		t.Fatal("generic prefix must rewrite")
	}
	if payload["model"] != "gpt-5.6-sol" {
		t.Fatalf("prefix not stripped: %v", payload["model"])
	}
}
