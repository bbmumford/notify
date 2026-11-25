package notify

import (
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1) // keep the :memory: DB on one connection
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close: %v", err)
		}
	})
	return db
}

// eachStore runs a case against both the MemStore reference and the durable SQLStore, holding the SQL
// store to the exact semantics MemStore defines. Cases set explicit CreatedAt/timestamps so both stores
// are deterministic without a clock seam.
func eachStore(t *testing.T, fn func(t *testing.T, s Store)) {
	t.Helper()
	t.Run("mem", func(t *testing.T) { fn(t, NewMemStore()) })
	t.Run("sql", func(t *testing.T) {
		s := NewSQLStore(openDB(t))
		if err := s.InitSchema(ctx); err != nil {
			t.Fatalf("InitSchema: %v", err)
		}
		fn(t, s)
	})
}
