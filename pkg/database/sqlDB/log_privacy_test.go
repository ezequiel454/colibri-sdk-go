// Modified in the ezequiel454 fork for forge M3; see FORK.md

package sqlDB

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"testing"

	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/config"
	"github.com/colibriproject-dev/colibri-sdk-go/pkg/base/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingDriver is a driver whose errors quote what real drivers quote: hosts, users, DSNs and
// values. No database is needed.
type failingDriver struct{ failBegin bool }

type driverErr struct{}

func (driverErr) Error() string {
	return `pq: password authentication failed for user "SENTINEL_USER" host=SENTINEL_HOST dsn=postgres://u:SENTINEL_PASS@h/db`
}
func (driverErr) SQLState() string { return "28P01" }

func (d failingDriver) Open(string) (driver.Conn, error) { return &failingConn{d}, nil }

type failingConn struct{ d failingDriver }

func (c *failingConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (c *failingConn) Close() error                        { return nil }
func (c *failingConn) Begin() (driver.Tx, error) {
	if c.d.failBegin {
		return nil, driverErr{}
	}
	return failingTx{}, nil
}

type failingTx struct{}

func (failingTx) Commit() error   { return driverErr{} }
func (failingTx) Rollback() error { return nil }

func init() {
	sql.Register("forge-failing-begin", failingDriver{failBegin: true})
	sql.Register("forge-failing-commit", failingDriver{})
}

func TestTransactionLogsNeverQuoteDriverOrCallerErrors(t *testing.T) {
	var buf bytes.Buffer
	env := config.ENVIRONMENT
	config.ENVIRONMENT = config.ENVIRONMENT_PRODUCTION
	logging.InitializeWith(&buf)
	t.Cleanup(func() { config.ENVIRONMENT = env; logging.InitializeWith(os.Stdout) })

	open := func(name string) *sql.DB {
		db, err := sql.Open(name, "")
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		return db
	}
	tx := NewTransaction().(*sqlTransaction)

	// begin fails
	require.Error(t, tx.ExecuteInInstance(context.Background(), open("forge-failing-begin"), func(context.Context) error { return nil }))
	// the callback fails with text that holds data from outside
	require.Error(t, tx.ExecuteInInstance(context.Background(), open("forge-failing-commit"), func(context.Context) error {
		return errors.New("SENTINEL_CALLER_ERROR for user SENTINEL_EMAIL")
	}))
	// commit fails
	require.Error(t, tx.ExecuteInInstance(context.Background(), open("forge-failing-commit"), func(context.Context) error { return nil }))

	out := buf.String()
	assert.NotContains(t, out, "SENTINEL")
	assert.NotContains(t, out, "password")
	assert.Contains(t, out, "could not start database transaction")
	assert.Contains(t, out, "error executing transaction")
	assert.Contains(t, out, "could not commit transaction")
	assert.Contains(t, out, `"sqlState":"28P01"`)
	assert.Contains(t, out, `"errorType"`)
}
