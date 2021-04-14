package driver_test

import (
	"database/sql"
	"reflect"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/driver"
)

type V = interface{}

var driverMu sync.Mutex
var drivers = map[driver.Provider]*driver.Driver{}

func sqlOpen(t *testing.T, provider driver.Provider, dsn string) *sql.DB {
	driverMu.Lock()
	drv, ok := drivers[provider]
	if !ok {
		drv = driver.New(provider, driver.Options{})
		drivers[provider] = drv
	}
	driverMu.Unlock()

	conn, err := drv.OpenConnector(dsn)
	require.NoError(t, err)
	return sql.OpenDB(conn)
}

type Pointers []V

func (ptrs Pointers) Values() []V {
	values := make([]V, len(ptrs))
	for i := range values {
		values[i] = reflect.ValueOf(ptrs[i]).Elem().Interface()
	}
	return values
}

type Records [][]V

func (records Records) Rows(rows ...int) Records {
	result := make(Records, len(rows))

	for i := range rows {
		result[i] = records[rows[i]]
	}

	return result
}

func (records Records) Columns(cols ...int) Records {
	result := make(Records, len(records))

	for i := range records {
		result[i] = make([]V, len(cols))
		for j := range cols {
			result[i][j] = records[i][cols[j]]
		}
	}

	return result
}
