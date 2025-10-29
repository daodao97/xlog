package storage

import (
	"context"
	"path/filepath"
	"testing"
)

func TestEnsureFTSIndexOnFreshDB(t *testing.T) {
	if testing.Short() {
		t.Skip("skip slow duckdb test in short mode")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "test.duckdb")
	store, err := NewDuckDB(path)
	if err != nil {
		t.Fatalf("NewDuckDB failed: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.Init(ctx); err != nil {
		t.Fatalf("Init failed: %v", err)
	}
	// ensureFTSIndex called inside Init; call again explicitly to verify idempotency.
	if err := store.ensureFTSIndex(ctx); err != nil {
		t.Fatalf("ensureFTSIndex failed: %v", err)
	}
}
