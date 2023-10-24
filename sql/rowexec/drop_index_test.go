// Copyright 2020-2021 Dolthub, Inc.
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

package rowexec

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	. "github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/go-mysql-server/test"
)

func TestDeleteIndex(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("foo")
	pro := memory.NewDBProvider(db)

	table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	}), nil)

	driver := new(mockDriver)
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", table)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	var expressions = []sql.Expression{
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(1, types.Int64, "db", "foo", "a", true),
	}

	done, ready, err := idxReg.AddIndex(&mockIndex{id: "idx", db: "foo", table: "foo", exprs: expressions})
	require.NoError(err)
	close(done)
	<-ready

	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	idxReg.ReleaseIndex(idx)

	di := NewDropIndex("idx", NewResolvedTable(table, nil, nil))
	di.Catalog = catalog
	di.CurrentDatabase = "foo"

	baseSession := sql.NewBaseSession()
	baseSession.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(baseSession, pro)))

	_, err = DefaultBuilder.Build(ctx, di, nil)
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Equal([]string{"idx"}, driver.deleted)
	require.Nil(idxReg.Index("foo", "idx"))
}

func TestDeleteIndexNotReady(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("foo")
	pro := memory.NewDBProvider(db)

	table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	}), nil)

	driver := new(mockDriver)
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", table)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	var expressions = []sql.Expression{
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(1, types.Int64, "db", "foo", "a", true),
	}

	baseSess := sql.NewBaseSession()
	baseSess.SetIndexRegistry(idxReg)

	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(baseSess, pro)))
	done, ready, err := idxReg.AddIndex(&mockIndex{id: "idx", db: "foo", table: "foo", exprs: expressions})
	require.NoError(err)

	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	idxReg.ReleaseIndex(idx)

	di := NewDropIndex("idx", NewResolvedTable(table, nil, nil))
	di.Catalog = catalog
	di.CurrentDatabase = "foo"

	_, err = DefaultBuilder.Build(ctx, di, nil)
	require.Error(err)
	require.True(ErrIndexNotAvailable.Is(err))

	time.Sleep(50 * time.Millisecond)

	require.Equal(([]string)(nil), driver.deleted)
	require.NotNil(idxReg.Index("foo", "idx"))

	close(done)
	<-ready
}

func TestDeleteIndexOutdated(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("foo")
	pro := memory.NewDBProvider(db)

	table := memory.NewTable(db.BaseDatabase, "foo", sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "a", Source: "foo"},
		{Name: "b", Source: "foo"},
		{Name: "c", Source: "foo"},
	}), nil)

	driver := new(mockDriver)
	idxReg := sql.NewIndexRegistry()
	idxReg.RegisterIndexDriver(driver)
	db.AddTable("foo", table)
	catalog := test.NewCatalog(sql.NewDatabaseProvider(db))

	var expressions = []sql.Expression{
		expression.NewGetFieldWithTable(0, types.Int64, "db", "foo", "c", true),
		expression.NewGetFieldWithTable(1, types.Int64, "db", "foo", "a", true),
	}

	baseSess := sql.NewBaseSession()
	baseSess.SetIndexRegistry(idxReg)
	ctx := sql.NewContext(context.Background(), sql.WithSession(memory.NewSession(baseSess, pro)))
	done, ready, err := idxReg.AddIndex(&mockIndex{id: "idx", db: "foo", table: "foo", exprs: expressions})
	require.NoError(err)
	close(done)
	<-ready

	idx := idxReg.Index("foo", "idx")
	require.NotNil(idx)
	idxReg.ReleaseIndex(idx)
	idxReg.MarkOutdated(idx)

	di := NewDropIndex("idx", NewResolvedTable(table, nil, nil))
	di.Catalog = catalog
	di.CurrentDatabase = "foo"

	_, err = DefaultBuilder.Build(ctx, di, nil)
	require.NoError(err)

	time.Sleep(50 * time.Millisecond)

	require.Equal([]string{"idx"}, driver.deleted)
	require.Nil(idxReg.Index("foo", "idx"))
}
