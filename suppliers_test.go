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
