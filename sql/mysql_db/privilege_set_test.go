// Copyright 2023 Dolthub, Inc.
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

package mysql_db

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
)

func TestClears(t *testing.T) {
	privSet := buildStdTestPrivs()
	require.True(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).Has(sql.PrivilegeType_Execute))
	privSet.ClearRoutine("db1", "rtn", true)
	require.False(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).Has(sql.PrivilegeType_Execute))
	require.False(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).HasPrivileges())

	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Create))
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Drop))
	privSet.ClearColumn("db1", "tbl", "col")
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Create))
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Drop))
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").HasPrivileges())

	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Update))
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Delete))
	privSet.ClearTable("db1", "tbl")
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Update))
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Delete))
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").HasPrivileges())

	require.True(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Select))
	require.True(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Insert))
	privSet.ClearDatabase("db1")
	require.False(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Select))
	require.False(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Insert))
	require.False(t, privSet.getUseableDb("db1").HasPrivileges())

	// Verify that clearing a DB deletes all the tables and routines hanging off of it.
	privSet = buildStdTestPrivs()
	privSet.ClearDatabase("db1")
	require.False(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).HasPrivileges())
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").HasPrivileges())
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").HasPrivileges())
	require.False(t, privSet.getUseableDb("db1").HasPrivileges())

	// Spot check that db2 wasn't affected.
	require.True(t, privSet.getUseableDb("db2").getUseableRoutine("rtn", true).Has(sql.PrivilegeType_Execute))
	require.True(t, privSet.getUseableDb("db2").getUseableTbl("tbl").Has(sql.PrivilegeType_Update))
	require.True(t, privSet.getUseableDb("db2").Has(sql.PrivilegeType_Select))

	// Verify that clearing a table deletes all the columns hanging off of it, but not routines
	privSet = buildStdTestPrivs()
	privSet.ClearTable("db1", "tbl")
	require.True(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).HasPrivileges())
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").HasPrivileges())
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").HasPrivileges())

	// Verify that clearing a routine doesn't affect anything else.
	privSet = buildStdTestPrivs()
	privSet.ClearRoutine("db1", "rtn", true)
	require.False(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).HasPrivileges())
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").HasPrivileges())
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").HasPrivileges())

	// Verify that clearing a column doesn't affect anything else.
	privSet = buildStdTestPrivs()
	privSet.ClearColumn("db1", "tbl", "col")
	require.True(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).HasPrivileges())
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").HasPrivileges())
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").HasPrivileges())
}

func TestRemoves(t *testing.T) {
	privSet := buildStdTestPrivs()
	require.True(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).Has(sql.PrivilegeType_Execute))
	privSet.RemoveRoutine("db1", "rtn", true, sql.PrivilegeType_Execute)
	require.False(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).Has(sql.PrivilegeType_Execute))
	require.False(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", true).HasPrivileges())

	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Create))
	privSet.RemoveColumn("db1", "tbl", "col", sql.PrivilegeType_Create)
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Create))
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Drop))
	privSet.RemoveColumn("db1", "tbl", "col", sql.PrivilegeType_Drop)
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Drop))
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").HasPrivileges())

	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Update))
	privSet.RemoveTable("db1", "tbl", sql.PrivilegeType_Update)
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Update))
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Delete))
	privSet.RemoveTable("db1", "tbl", sql.PrivilegeType_Delete)
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Delete))
	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").HasPrivileges())

	require.True(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Select))
	privSet.RemoveDatabase("db1", sql.PrivilegeType_Select)
	require.False(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Select))
	require.True(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Insert))
	privSet.RemoveDatabase("db1", sql.PrivilegeType_Insert)
	require.False(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Insert))
	require.False(t, privSet.getUseableDb("db1").HasPrivileges())
}

func TestUnions(t *testing.T) {
	privSet := buildStdTestPrivs()
	require.False(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Execute))

	addInSet := NewPrivilegeSet()
	addInSet.AddDatabase("db1", sql.PrivilegeType_Execute)

	privSet.UnionWith(addInSet)
	require.NotEqual(t, privSet, addInSet)
	require.False(t, privSet.Equals(addInSet))
	require.True(t, privSet.getUseableDb("db1").Has(sql.PrivilegeType_Execute))

	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Execute))
	addInSet.AddTable("db1", "tbl", sql.PrivilegeType_Execute)
	privSet.UnionWith(addInSet)
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").Has(sql.PrivilegeType_Execute))

	require.False(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Execute))
	addInSet.AddColumn("db1", "tbl", "col", sql.PrivilegeType_Execute)
	privSet.UnionWith(addInSet)
	require.True(t, privSet.getUseableDb("db1").getUseableTbl("tbl").getUseableCol("col").Has(sql.PrivilegeType_Execute))

	require.False(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", false).Has(sql.PrivilegeType_Execute))
	addInSet.AddRoutine("db1", "rtn", false, sql.PrivilegeType_Execute)
	privSet.UnionWith(addInSet)
	require.True(t, privSet.getUseableDb("db1").getUseableRoutine("rtn", false).Has(sql.PrivilegeType_Execute))

	// Verify that the union of two identical sets is the same as the original set.
}

// Verify that the basics of the test object behave as expected. By running this test, we can be more terse
// in the other tests.
func TestCommonHarness(t *testing.T) {

	privSet := buildStdTestPrivs()

	copySet := privSet.Copy()

	// compare the two sets, and make sure they are equal but not the same object.
	require.Equal(t, privSet, &copySet)
	require.False(t, privSet == &copySet)
	require.True(t, privSet.Equals(copySet))

	require.True(t, privSet.HasPrivileges())

	var verify = func(set *PrivilegeSet, dbName string) {
		db := set.getUseableDb(dbName)
		require.True(t, db.HasPrivileges())
		require.True(t, db.Has(sql.PrivilegeType_Select))
		require.True(t, db.Has(sql.PrivilegeType_Insert))
		require.False(t, db.Has(sql.PrivilegeType_Execute))

		tbl := db.getUseableTbl("tbl")
		require.True(t, tbl.HasPrivileges())
		require.True(t, tbl.Has(sql.PrivilegeType_Update))
		require.True(t, tbl.Has(sql.PrivilegeType_Delete))
		require.False(t, tbl.Has(sql.PrivilegeType_Execute))

		col := tbl.getUseableCol("col")
		require.True(t, col.HasPrivileges())
		require.True(t, col.Has(sql.PrivilegeType_Create))
		require.True(t, col.Has(sql.PrivilegeType_Drop))
		require.False(t, col.Has(sql.PrivilegeType_Execute))

		rtn := db.getUseableRoutine("rtn", true)
		require.True(t, rtn.HasPrivileges())
		require.True(t, rtn.Has(sql.PrivilegeType_Execute))
		require.False(t, rtn.Has(sql.PrivilegeType_Create))

		// Make sure non-existent entities are handled as no privs.
		nodb := set.getUseableDb("nodb")
		require.False(t, nodb.HasPrivileges())

		notbl := db.getUseableTbl("notbl")
		require.False(t, notbl.HasPrivileges())

		nocol := tbl.getUseableCol("nocol")
		require.False(t, nocol.HasPrivileges())

		nortn := db.getUseableRoutine("noroutine", true)
		require.False(t, nortn.HasPrivileges())
		nortn = db.getUseableRoutine("noroutine", false)
		require.False(t, nortn.HasPrivileges())
	}

	verify(privSet, "db1")
	verify(privSet, "db2")
	verify(&copySet, "db1")
	verify(&copySet, "db2")
}

func buildStdTestPrivs() *PrivilegeSet {
	db1Set := buildSingleDbWithName("db1")
	db2Set := buildSingleDbWithName("db2")

	db1Set.UnionWith(*db2Set)

	return db1Set
}

func buildSingleDbWithName(dbName string) *PrivilegeSet {
	privSet := buildDBTableSet(dbName, "tbl")

	privSet.AddGlobalStatic(sql.PrivilegeType_Super)
	privSet.AddRoutine(dbName, "rtn", true, sql.PrivilegeType_Execute)
	privSet.AddColumn(dbName, "tbl", "col", sql.PrivilegeType_Create, sql.PrivilegeType_Drop)

	return privSet
}

func buildDBTableSet(dbName, tblName string) *PrivilegeSet {
	privSet := NewPrivilegeSet()

	privSet.AddDatabase(dbName, sql.PrivilegeType_Select, sql.PrivilegeType_Insert)
	privSet.AddTable(dbName, tblName, sql.PrivilegeType_Update, sql.PrivilegeType_Delete)

	return &privSet
}
