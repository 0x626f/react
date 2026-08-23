package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const postgresTestURLVariable = "OUTBOX_POSTGRES_TEST_URL"

func TestPostgresStoreContract(t *testing.T) {
	url := requireOutboxIntegrationURL(t, postgresTestURLVariable, "PostgreSQL")
	runStoreContract(t, func(t testing.TB) testHarness {
		pool, store, namespace := newPostgresIntegrationStore(t, url)
		t.Cleanup(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM react_outbox.records WHERE namespace=$1`, namespace)
			_ = store.Close()
			pool.Close()
		})
		return testHarness{
			Store: store,
			Time: testWallTimeDriver{NowFunc: func(ctx context.Context) (time.Time, error) {
				var now time.Time
				err := pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
				return CanonicalTime(now), err
			}},
			Capabilities: testCapabilities{AllQueryCombinations: true, SameResourceDomainComposition: true},
		}
	})
}

func TestPostgresTransactionBoundAppendCommitAndRollback(t *testing.T) {
	url := requireOutboxIntegrationURL(t, postgresTestURLVariable, "PostgreSQL")
	pool, store, namespace := newPostgresIntegrationStore(t, url)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		// Keep cleanup namespace-scoped even on a user-supplied integration
		// database; never drop a table that might predate this test run.
		_, _ = pool.Exec(ctx, `DELETE FROM react_outbox.domain_state_test WHERE namespace=$1`, namespace)
		_, _ = pool.Exec(ctx, `DELETE FROM react_outbox.records WHERE namespace=$1`, namespace)
		_ = store.Close()
		pool.Close()
	})
	ctx := t.Context()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS react_outbox.domain_state_test (namespace text, id text, value text, PRIMARY KEY(namespace,id))`); err != nil {
		t.Fatal(err)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO react_outbox.domain_state_test(namespace,id,value) VALUES($1,'rollback','value')`, namespace); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Bind(tx).Append(ctx, testRecord(testWithID("rollback-outbox"))); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, pool, store, namespace, "rollback", "rollback-outbox", 0)

	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO react_outbox.domain_state_test(namespace,id,value) VALUES($1,'commit','value')`, namespace); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Bind(tx).Append(ctx, testRecord(testWithID("commit-outbox"))); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	assertCounts(t, pool, store, namespace, "commit", "commit-outbox", 1)

	// A rejected bound batch rolls back only its savepoint and leaves the
	// caller-owned domain transaction usable.
	tx, err = pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `INSERT INTO react_outbox.domain_state_test(namespace,id,value) VALUES($1,'savepoint','before')`, namespace); err != nil {
		t.Fatal(err)
	}
	_, err = store.Bind(tx).Append(ctx,
		testRecord(testWithID("savepoint-must-rollback")),
		testRecord(testWithID("commit-outbox")),
	)
	if !errors.Is(err, ErrDuplicateID) {
		t.Fatalf("bound conflicting batch error = %v, want ErrDuplicateID", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE react_outbox.domain_state_test SET value='after' WHERE namespace=$1 AND id='savepoint'`, namespace); err != nil {
		t.Fatalf("outer transaction was left unusable: %v", err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var domainCount int
	if err = pool.QueryRow(ctx, `SELECT COUNT(*) FROM react_outbox.domain_state_test WHERE namespace=$1 AND id='savepoint'`, namespace).Scan(&domainCount); err != nil || domainCount != 1 {
		t.Fatalf("savepoint domain count = %d, %v; want 1", domainCount, err)
	}
	if _, err = store.Get(ctx, "savepoint-must-rollback"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rejected bound batch left a row: %v", err)
	}
}

func TestPostgresMigratedConstraintsRejectInvalidStateShape(t *testing.T) {
	url := requireOutboxIntegrationURL(t, postgresTestURLVariable, "PostgreSQL")
	pool, store, namespace := newPostgresIntegrationStore(t, url)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, `DELETE FROM react_outbox.records WHERE namespace=$1`, namespace)
		_ = store.Close()
		pool.Close()
	})
	if _, err := store.Append(t.Context(), testRecord(testWithID("constraint-record"))); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `UPDATE react_outbox.records SET state='unknown' WHERE namespace=$1 AND id='constraint-record'`, namespace); err == nil {
		t.Fatal("state constraint accepted an unknown state")
	}
	if _, err := pool.Exec(t.Context(), `UPDATE react_outbox.records SET state='leased' WHERE namespace=$1 AND id='constraint-record'`, namespace); err == nil {
		t.Fatal("lease-shape constraint accepted missing fence fields")
	}
	if _, err := pool.Exec(t.Context(), `UPDATE react_outbox.records SET max_attempts=0 WHERE namespace=$1 AND id='constraint-record'`, namespace); err == nil {
		t.Fatal("attempt constraint accepted max_attempts=0")
	}
}

func TestPostgresSkipLockedAcrossConnections(t *testing.T) {
	url := requireOutboxIntegrationURL(t, postgresTestURLVariable, "PostgreSQL")
	pool, store, namespace := newPostgresIntegrationStore(t, url)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, `DELETE FROM react_outbox.records WHERE namespace=$1`, namespace)
		pool.Close()
	})
	ctx := t.Context()
	if _, err := store.Append(ctx,
		testRecord(testWithID("locked-a")),
		testRecord(testWithID("locked-b")),
	); err != nil {
		t.Fatal(err)
	}
	lockingTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lockingTx.Rollback(context.Background())
	var locked ID
	if err = lockingTx.QueryRow(ctx, `SELECT id FROM react_outbox.records
		WHERE namespace=$1 AND state='pending' ORDER BY available_at,created_at,id
		LIMIT 1 FOR UPDATE`, namespace).Scan(&locked); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.Claim(ctx, ClaimRequest{Owner: "other-connection", Limit: 2, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID == locked {
		t.Fatalf("locked=%q claimed=%#v", locked, claimed)
	}
}

func TestPostgresDueClaimQueryPlanUsesFocusedIndex(t *testing.T) {
	url := requireOutboxIntegrationURL(t, postgresTestURLVariable, "PostgreSQL")
	pool, store, namespace := newPostgresIntegrationStore(t, url)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(ctx, `DELETE FROM react_outbox.records WHERE namespace=$1`, namespace)
		pool.Close()
	})
	inputs := make([]NewRecord, 100)
	for index := range inputs {
		inputs[index] = testRecord(testWithID(ID(fmt.Sprintf("plan-%03d", index))))
	}
	if _, err := store.Append(t.Context(), inputs...); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(t.Context(), `SET LOCAL enable_seqscan=off`); err != nil {
		t.Fatal(err)
	}
	// Disable an explicit sort so this test verifies that the due index can
	// satisfy the stable claim ordering, independent of small-fixture costs.
	if _, err = tx.Exec(t.Context(), `SET LOCAL enable_sort=off`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(t.Context(), `EXPLAIN (FORMAT TEXT) SELECT id FROM react_outbox.records
		WHERE namespace=$1 AND state='pending' AND available_at <= clock_timestamp() AND attempts < max_attempts
		ORDER BY available_at,created_at,id FOR UPDATE SKIP LOCKED LIMIT 10`, namespace)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var plan strings.Builder
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		plan.WriteString(line)
		plan.WriteByte('\n')
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(plan.String(), "records_claim_idx") {
		t.Fatalf("due claim plan did not use records_claim_idx:\n%s", plan.String())
	}
}

func newPostgresIntegrationStore(t testing.TB, url string) (*pgxpool.Pool, *PostgresStore, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("PostgreSQL Ping: %v", err)
	}
	namespace := fmt.Sprintf("test-%d", time.Now().UnixNano())
	config := DefaultPostgresConfig()
	config.Namespace = namespace
	store, err := NewPostgresStore(pool, config)
	if err != nil {
		pool.Close()
		t.Fatalf("NewPostgresStore: %v", err)
	}
	if err = store.Migrate(ctx); err != nil {
		pool.Close()
		t.Fatalf("Migrate: %v", err)
	}
	return pool, store, namespace
}

func assertCounts(t testing.TB, pool *pgxpool.Pool, store *PostgresStore, namespace, domainID string, recordID ID, want int) {
	t.Helper()
	var count int
	if err := pool.QueryRow(t.Context(), `SELECT COUNT(*) FROM react_outbox.domain_state_test WHERE namespace=$1 AND id=$2`, namespace, domainID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("domain count = %d, want %d", count, want)
	}
	_, err := store.Get(t.Context(), recordID)
	if want == 0 && err == nil {
		t.Fatal("outbox row committed unexpectedly")
	}
	if want == 1 && err != nil {
		t.Fatalf("outbox row missing: %v", err)
	}
}
