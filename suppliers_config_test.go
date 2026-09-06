package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSuppliersRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	s := defaultSettings()
	s.Suppliers = []Supplier{{ID: "local-m365", Name: "M365", BaseURL: "http://127.0.0.1:4141/v1", APIKey: "k", Models: []string{"gpt-5.6-sol"}, Enabled: true}}
	if err := s.save(path); err != nil {
		t.Fatal(err)
	}
	loaded := loadSettings(path)
	if len(loaded.Suppliers) != 1 || loaded.Suppliers[0].ID != "local-m365" {
		t.Fatalf("suppliers lost: %+v", loaded.Suppliers)
	}
	if got := loaded.Suppliers[0]; got.APIKey != "k" || len(got.Models) != 1 {
		t.Fatalf("supplier fields lost: %+v", got)
	}
	if old := defaultSettings(); old.Suppliers != nil {
		t.Fatal("default suppliers must be nil for zero regression")
	}
}

func TestSuppliersInputOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.json")
	s := defaultSettings()
	s.SuppliersInput = `[{"id":"nb","base_url":"http://x/v1"}]`
	if err := s.save(path); err != nil {
		t.Fatal(err)
	}
	loaded := loadSettings(path)
	if len(loaded.Suppliers) != 1 || loaded.Suppliers[0].ID != "nb" {
		t.Fatalf("SuppliersInput not applied: %+v", loaded.Suppliers)
	}
}

func TestBadSuppliersInputIgnored(t *testing.T) {
	s := defaultSettings()
	s.SuppliersInput = `not json`
	_ = os.Setenv("SUPPLIERS_JSON", "")
	normalized := s.normalized()
	if normalized.Suppliers != nil {
		t.Fatalf("bad input must be ignored: %+v", normalized.Suppliers)
	}
}
