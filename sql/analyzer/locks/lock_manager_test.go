// Copyright 2022 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package locks_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/analyzer/locks"
)

const dbName = "db"

func getCatalog() sql.Catalog {
	db := memory.NewDatabase(dbName)
	t1 := memory.NewTable("foo", sql.PrimaryKeySchema{})
	t2 := memory.NewTable("bar", sql.PrimaryKeySchema{})
	t3 := memory.NewTable("baz", sql.PrimaryKeySchema{})
	db.AddTable("foo", t1)
	db.AddTable("bar", t2)
	db.AddTable("baz", t3)

	catalog := analyzer.NewCatalog(sql.NewDatabaseProvider(db))

	return catalog
}

func TestLockBehavior(t *testing.T) {
	c1 := sql.NewEmptyContext()
	c2 := sql.NewEmptyContext()

	// Initialize sessions so each ctx has a different pic
	c1.Session = sql.NewBaseSession()
	c2.Session = sql.NewBaseSession()

	c1.SetCurrentDatabase(dbName)
	c2.SetCurrentDatabase(dbName)

	// Initialize the lock manager
	lm := locks.NewLockManager(getCatalog())

	sleepDelta := 5 * time.Second
	startTime := time.Now()

	var err error
	go func() {
		err = lm.LockTable(c1, dbName, "foo")
		require.NoError(t, err)

		time.Sleep(sleepDelta)

		err = lm.UnlockTable(c1, dbName, "foo")
		require.NoError(t, err)
	}()

	time.Sleep(10 * time.Millisecond) // sleep for 10 milliseconds for the other routine

	err = lm.LockTable(c2, dbName, "foo")
	require.NoError(t, err)

	delta := time.Since(startTime)

	require.InDelta(t, delta, sleepDelta, float64(time.Second))
}
