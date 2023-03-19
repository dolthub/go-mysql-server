// Copyright 2021 Dolthub, Inc.
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

package analyzer

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gabereiser/go-mysql-server/sql"
)

func TestCatalogLockTable(t *testing.T) {
	require := require.New(t)
	c := NewCatalog(NewDatabaseProvider())

	ctx1 := sql.NewContext(context.Background())
	ctx1.SetCurrentDatabase("db1")
	ctx2 := sql.NewContext(context.Background())
	ctx2.SetCurrentDatabase("db1")

	c.LockTable(ctx1, "foo")
	c.LockTable(ctx2, "bar")
	c.LockTable(ctx1, "baz")
	ctx1.SetCurrentDatabase("db2")
	c.LockTable(ctx1, "qux")

	expected := sessionLocks{
		ctx1.ID(): dbLocks{
			"db1": tableLocks{
				"foo": struct{}{},
				"baz": struct{}{},
			},
			"db2": tableLocks{
				"qux": struct{}{},
			},
		},
		ctx2.ID(): dbLocks{
			"db1": tableLocks{
				"bar": struct{}{},
			},
		},
	}

	require.Equal(expected, c.locks)
}
