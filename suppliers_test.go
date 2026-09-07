package main

import "testing"

func TestSplitSupplierPrefix(t *testing.T) {
	sup, real, ok := SplitSupplierPrefix("m365/gpt-5.6")
	if !ok || sup != "m365" || real != "gpt-5.6" {
		t.Fatalf("got %q %q %v", sup, real, ok)
	}
	if _, _, ok := SplitSupplierPrefix("plain-model"); ok {
		t.Fatal("bare model must not split")
	}
	if _, _, ok := SplitSupplierPrefix("/leading"); ok {
		t.Fatal("empty supplier must not split")
	}
	if _, _, ok := SplitSupplierPrefix("trailing/"); ok {
		t.Fatal("empty model must not split")
	}
}

func TestSupplierValidate(t *testing.T) {
	bad := Supplier{ID: "Bad ID!", BaseURL: "://bad"}
	if err := bad.Validate(); err == nil {
		t.Fatal("expected validation error")
	}
	ok := Supplier{ID: "m365", Name: "M365", BaseURL: "http://127.0.0.1:4141/v1", APIKey: "k", Models: []string{"gpt-5.6-sol"}, Enabled: true}
	if err := ok.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFormatAutoPools(t *testing.T) {
	got := formatAutoPools(map[string][]string{"auto": {"m365/gpt-5.6-sol", "opencode/big-pickle"}})
	if got != "auto=m365/gpt-5.6-sol,opencode/big-pickle" {
		t.Fatalf("bad format: %q", got)
	}
	if formatAutoPools(nil) != "" {
		t.Fatal("nil pools must format empty")
	}
}

func TestSuppliersSeedText(t *testing.T) {
	s := defaultSettings()
	if suppliersSeedText(s) != "[]" {
		t.Fatal("empty settings must seed []")
	}
	s.SuppliersInput = `[{"id":"a"}]`
	if suppliersSeedText(s) != `[{"id":"a"}]` {
		t.Fatal("raw input must win")
	}
}

func TestParseAccountLines(t *testing.T) {
	lines := parseAccountLines("# 注释\na@b.c, p1\n\nc@d.e,p2\r\nbadline\n,empty\n")
	if len(lines) != 2 || lines[0][0] != "a@b.c" || lines[0][1] != "p1" || lines[1][0] != "c@d.e" {
		t.Fatalf("bad parse: %q", lines)
	}
	if len(parseAccountLines("")) != 0 {
		t.Fatal("empty input must yield nothing")
	}
}

func TestParseSuppliersJSON(t *testing.T) {
	parsed, err := ParseSuppliersJSON(`[{"id":"local-m365","base_url":"http://127.0.0.1:4141/v1","models":["a"]}]`)
	if err != nil {
		t.Fatal(err)
	}
	if len(parsed) != 1 || parsed[0].ID != "local-m365" {
		t.Fatalf("unexpected parse result: %+v", parsed)
	}
	for _, reserved := range []string{"opencode", "cline", "m365"} {
		if _, err := ParseSuppliersJSON(`[{"id":"` + reserved + `","base_url":"http://x/v1"}]`); err == nil {
			t.Fatalf("reserved id %s must be rejected", reserved)
		}
	}
	if _, err := ParseSuppliersJSON(`not json`); err == nil {
		t.Fatal("invalid json must be rejected")
	}
	if parsed, err := ParseSuppliersJSON(``); err != nil || parsed != nil {
		t.Fatalf("empty input must yield nil,nil: %v %+v", err, parsed)
	}
}
