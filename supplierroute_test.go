package main

import "testing"

func TestResolveAutoPool(t *testing.T) {
	g := &gateway{cfg: config{autoPools: map[string][]string{"auto": {"local-m365/gpt-5.6-sol", "opencode/big-pickle"}}}}
	req := upstreamRequest{body: []byte(`{"model":"auto","messages":[]}`)}
	chain := g.resolveUnifiedChain(req)
	if len(chain) != 2 || chain[0] != "local-m365/gpt-5.6-sol" {
		t.Fatalf("auto not expanded: %v", chain)
	}
	req = upstreamRequest{body: []byte(`{"model":"auto/code","messages":[]}`)}
	g.cfg.autoPools["code"] = []string{"local-m365/gpt-5.6-luna"}
	chain = g.resolveUnifiedChain(req)
	if len(chain) != 1 || chain[0] != "local-m365/gpt-5.6-luna" {
		t.Fatalf("named pool not expanded: %v", chain)
	}
}

func TestParseAutoPools(t *testing.T) {
	pools := parseAutoPools("auto=a/b,opencode/big-pickle;code=m365/x")
	if len(pools["auto"]) != 2 || pools["auto"][0] != "a/b" {
		t.Fatalf("bad parse: %v", pools)
	}
	if len(pools["code"]) != 1 {
		t.Fatalf("bad parse: %v", pools)
	}
	if len(parseAutoPools("")) != 0 {
		t.Fatal("empty input must yield empty map")
	}
}

func TestGenericPrefixPassthrough(t *testing.T) {
	g := &gateway{cfg: config{suppliers: []Supplier{{ID: "local-m365", BaseURL: "http://127.0.0.1:4141/v1", APIKey: "k", Enabled: true}}}}
	sup, ok := g.lookupSupplier("local-m365/gpt-5.6-sol")
	if !ok || sup.BaseURL != "http://127.0.0.1:4141/v1" {
		t.Fatal("supplier lookup failed")
	}
	if _, ok := g.lookupSupplier("unknown/x"); ok {
		t.Fatal("unknown supplier must not match")
	}
	if _, ok := g.lookupSupplier("plain"); ok {
		t.Fatal("bare model must not match")
	}
}
