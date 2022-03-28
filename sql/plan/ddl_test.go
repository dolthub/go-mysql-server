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

package plan

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestCreateTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	tables := db.Tables()
	_, ok := tables["testTable"]
	require.False(ok)

	s := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	})

	require.NoError(createTable(t, db, "testTable", s, IfNotExistsAbsent, IsTempTableAbsent))

	tables = db.Tables()

	newTable, ok := tables["testTable"]
	require.True(ok)

	require.Equal(newTable.Schema(), s.Schema)

	for _, s := range newTable.Schema() {
		require.Equal("testTable", s.Source)
	}

	require.Error(createTable(t, db, "testTable", s, IfNotExistsAbsent, IsTempTableAbsent))
	require.NoError(createTable(t, db, "testTable", s, IfNotExists, IsTempTableAbsent))
}

func TestDropTable(t *testing.T) {
	require := require.New(t)

	db := memory.NewDatabase("test")
	ctx := sql.NewEmptyContext()

	s := sql.NewPrimaryKeySchema(sql.Schema{
		{Name: "c1", Type: sql.Text},
		{Name: "c2", Type: sql.Int32},
	})

	require.NoError(createTable(t, db, "testTable1", s, IfNotExistsAbsent, IsTempTableAbsent))
	require.NoError(createTable(t, db, "testTable2", s, IfNotExistsAbsent, IsTempTableAbsent))
	require.NoError(createTable(t, db, "testTable3", s, IfNotExistsAbsent, IsTempTableAbsent))

	d := NewDropTable([]sql.Node{NewResolvedTable(memory.NewTable("testTable1", s, db.GetForeignKeyCollection()), db, nil), NewResolvedTable(memory.NewTable("testTable2", s, db.GetForeignKeyCollection()), db, nil)}, false)
	rows, err := d.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	r, err := rows.Next(ctx)
	require.Equal(err, io.EOF)
	require.Nil(r)

	_, ok := db.Tables()["testTable1"]
	require.False(ok)
	_, ok = db.Tables()["testTable2"]
	require.False(ok)
	_, ok = db.Tables()["testTable3"]
	require.True(ok)

	d = NewDropTable([]sql.Node{NewResolvedTable(memory.NewTable("testTable1", s, db.GetForeignKeyCollection()), db, nil)}, false)
	_, err = d.RowIter(sql.NewEmptyContext(), nil)
	require.Error(err)

	d = NewDropTable([]sql.Node{NewResolvedTable(memory.NewTable("testTable3", s, db.GetForeignKeyCollection()), db, nil)}, false)
	_, err = d.RowIter(sql.NewEmptyContext(), nil)
	require.NoError(err)

	_, ok = db.Tables()["testTable3"]
	require.False(ok)
}

func createTable(t *testing.T, db sql.Database, name string, schema sql.PrimaryKeySchema, ifNotExists IfNotExistsOption, temporary TempTableOption) error {
	c := NewCreateTable(db, name, ifNotExists, temporary, &TableSpec{Schema: schema})

	rows, err := c.RowIter(sql.NewEmptyContext(), nil)
	if err != nil {
		return err
	}

	ctx := sql.NewEmptyContext()
	r, err := rows.Next(ctx)
	require.Nil(t, r)
	require.Equal(t, io.EOF, err)
	return nil
}
