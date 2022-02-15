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

package grant_tables

import (
	"fmt"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

const dbTblName = "db"

var (
	errDbEntry = fmt.Errorf("the converter for the `db` table was given an unknown entry")
	errDbRow   = fmt.Errorf("the converter for the `db` table was given a row belonging to an unknown schema")

	dbTblSchema sql.Schema
)

// DbConverter handles the conversion between a stored *User entry and the faux "db" Grant Table.
type DbConverter struct{}

var _ in_mem_table.DataEditorConverter = DbConverter{}

// RowToKey implements the interface in_mem_table.DataEditorConverter.
func (conv DbConverter) RowToKey(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(dbTblSchema) {
		return nil, errDbRow
	}
	host, ok := row[dbTblColIndex_Host].(string)
	if !ok {
		return nil, errDbRow
	}
	user, ok := row[dbTblColIndex_User].(string)
	if !ok {
		return nil, errDbRow
	}
	return UserPrimaryKey{
		Host: host,
		User: user,
	}, nil
}

// AddRowToEntry implements the interface in_mem_table.DataEditorConverter.
func (conv DbConverter) AddRowToEntry(ctx *sql.Context, row sql.Row, entry in_mem_table.Entry) (in_mem_table.Entry, error) {
	if len(row) != len(dbTblSchema) {
		return nil, errDbRow
	}
	user, ok := entry.(*User)
	if !ok {
		return nil, errDbEntry
	}
	user = user.Copy(ctx).(*User)

	db, ok := row[dbTblColIndex_Db].(string)
	if !ok {
		return nil, errDbRow
	}
	var privs []sql.PrivilegeType
	for i, val := range row {
		if strVal, ok := val.(string); ok && strVal == "Y" {
			switch i {
			case dbTblColIndex_Select_priv:
				privs = append(privs, sql.PrivilegeType_Select)
			case dbTblColIndex_Insert_priv:
				privs = append(privs, sql.PrivilegeType_Insert)
			case dbTblColIndex_Update_priv:
				privs = append(privs, sql.PrivilegeType_Update)
			case dbTblColIndex_Delete_priv:
				privs = append(privs, sql.PrivilegeType_Delete)
			case dbTblColIndex_Create_priv:
				privs = append(privs, sql.PrivilegeType_Create)
			case dbTblColIndex_Drop_priv:
				privs = append(privs, sql.PrivilegeType_Drop)
			case dbTblColIndex_Grant_priv:
				privs = append(privs, sql.PrivilegeType_Grant)
			case dbTblColIndex_References_priv:
				privs = append(privs, sql.PrivilegeType_References)
			case dbTblColIndex_Index_priv:
				privs = append(privs, sql.PrivilegeType_Index)
			case dbTblColIndex_Alter_priv:
				privs = append(privs, sql.PrivilegeType_Alter)
			case dbTblColIndex_Create_tmp_table_priv:
				privs = append(privs, sql.PrivilegeType_CreateTempTable)
			case dbTblColIndex_Lock_tables_priv:
				privs = append(privs, sql.PrivilegeType_LockTables)
			case dbTblColIndex_Create_view_priv:
				privs = append(privs, sql.PrivilegeType_CreateView)
			case dbTblColIndex_Show_view_priv:
				privs = append(privs, sql.PrivilegeType_ShowView)
			case dbTblColIndex_Create_routine_priv:
				privs = append(privs, sql.PrivilegeType_CreateRoutine)
			case dbTblColIndex_Alter_routine_priv:
				privs = append(privs, sql.PrivilegeType_AlterRoutine)
			case dbTblColIndex_Execute_priv:
				privs = append(privs, sql.PrivilegeType_Execute)
			case dbTblColIndex_Event_priv:
				privs = append(privs, sql.PrivilegeType_Event)
			case dbTblColIndex_Trigger_priv:
				privs = append(privs, sql.PrivilegeType_Trigger)
			}
		}
	}
	user.PrivilegeSet.AddDatabase(db, privs...)
	return user, nil
}

// RemoveRowFromEntry implements the interface in_mem_table.DataEditorConverter.
func (conv DbConverter) RemoveRowFromEntry(ctx *sql.Context, row sql.Row, entry in_mem_table.Entry) (in_mem_table.Entry, error) {
	if len(row) != len(dbTblSchema) {
		return nil, errDbRow
	}
	user, ok := entry.(*User)
	if !ok {
		return nil, errDbEntry
	}
	user = user.Copy(ctx).(*User)

	db, ok := row[dbTblColIndex_Db].(string)
	if !ok {
		return nil, errDbRow
	}
	user.PrivilegeSet.ClearDatabase(db)
	return user, nil
}

// EntryToRows implements the interface in_mem_table.DataEditorConverter.
func (conv DbConverter) EntryToRows(ctx *sql.Context, entry in_mem_table.Entry) ([]sql.Row, error) {
	user, ok := entry.(*User)
	if !ok {
		return nil, errDbEntry
	}

	var rows []sql.Row
	for _, dbSet := range user.PrivilegeSet.GetDatabases() {
		if dbSet.Count() == 0 {
			continue
		}
		row := make(sql.Row, len(dbTblSchema))
		var err error
		for i, col := range dbTblSchema {
			row[i], err = col.Default.Eval(ctx, nil)
			if err != nil {
				return nil, err // Should never happen, schema is static
			}
		}
		row[dbTblColIndex_User] = user.User
		row[dbTblColIndex_Host] = user.Host
		row[dbTblColIndex_Db] = dbSet.Name()

		for _, priv := range dbSet.ToSlice() {
			switch priv {
			case sql.PrivilegeType_Select:
				row[dbTblColIndex_Select_priv] = "Y"
			case sql.PrivilegeType_Insert:
				row[dbTblColIndex_Insert_priv] = "Y"
			case sql.PrivilegeType_Update:
				row[dbTblColIndex_Update_priv] = "Y"
			case sql.PrivilegeType_Delete:
				row[dbTblColIndex_Delete_priv] = "Y"
			case sql.PrivilegeType_Create:
				row[dbTblColIndex_Create_priv] = "Y"
			case sql.PrivilegeType_Drop:
				row[dbTblColIndex_Drop_priv] = "Y"
			case sql.PrivilegeType_Grant:
				row[dbTblColIndex_Grant_priv] = "Y"
			case sql.PrivilegeType_References:
				row[dbTblColIndex_References_priv] = "Y"
			case sql.PrivilegeType_Index:
				row[dbTblColIndex_Index_priv] = "Y"
			case sql.PrivilegeType_Alter:
				row[dbTblColIndex_Alter_priv] = "Y"
			case sql.PrivilegeType_CreateTempTable:
				row[dbTblColIndex_Create_tmp_table_priv] = "Y"
			case sql.PrivilegeType_LockTables:
				row[dbTblColIndex_Lock_tables_priv] = "Y"
			case sql.PrivilegeType_CreateView:
				row[dbTblColIndex_Create_view_priv] = "Y"
			case sql.PrivilegeType_ShowView:
				row[dbTblColIndex_Show_view_priv] = "Y"
			case sql.PrivilegeType_CreateRoutine:
				row[dbTblColIndex_Create_routine_priv] = "Y"
			case sql.PrivilegeType_AlterRoutine:
				row[dbTblColIndex_Alter_routine_priv] = "Y"
			case sql.PrivilegeType_Execute:
				row[dbTblColIndex_Execute_priv] = "Y"
			case sql.PrivilegeType_Event:
				row[dbTblColIndex_Event_priv] = "Y"
			case sql.PrivilegeType_Trigger:
				row[dbTblColIndex_Trigger_priv] = "Y"
			}
		}
		rows = append(rows, row)
	}

	return rows, nil
}

// init creates the schema for the "db" Grant Table.
func init() {
	// Types
	char32_utf8_bin := sql.MustCreateString(sqltypes.Char, 32, sql.Collation_utf8_bin)
	char64_utf8_bin := sql.MustCreateString(sqltypes.Char, 64, sql.Collation_utf8_bin)
	char255_ascii_general_ci := sql.MustCreateString(sqltypes.Char, 255, sql.Collation_ascii_general_ci)
	enum_N_Y_utf8_general_ci := sql.MustCreateEnumType([]string{"N", "Y"}, sql.Collation_utf8_general_ci)

	// Column Templates
	char32_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char32_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char32_utf8_bin), char32_utf8_bin, true, false),
		Nullable: false,
	}
	char64_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char64_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char64_utf8_bin), char64_utf8_bin, true, false),
		Nullable: false,
	}
	char255_ascii_general_ci_not_null_default_empty := &sql.Column{
		Type:     char255_ascii_general_ci,
		Default:  mustDefault(expression.NewLiteral("", char255_ascii_general_ci), char255_ascii_general_ci, true, false),
		Nullable: false,
	}
	enum_N_Y_utf8_general_ci_not_null_default_N := &sql.Column{
		Type:     enum_N_Y_utf8_general_ci,
		Default:  mustDefault(expression.NewLiteral("N", enum_N_Y_utf8_general_ci), enum_N_Y_utf8_general_ci, true, false),
		Nullable: false,
	}

	dbTblSchema = sql.Schema{
		columnTemplate("Host", dbTblName, true, char255_ascii_general_ci_not_null_default_empty),
		columnTemplate("Db", dbTblName, true, char64_utf8_bin_not_null_default_empty),
		columnTemplate("User", dbTblName, true, char32_utf8_bin_not_null_default_empty),
		columnTemplate("Select_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Insert_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Update_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Delete_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Drop_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Grant_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("References_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Index_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Alter_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_tmp_table_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Lock_tables_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_view_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Show_view_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_routine_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Alter_routine_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Execute_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Event_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Trigger_priv", dbTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
	}
}

// These represent the column indexes of the "db" Grant Table.
const (
	dbTblColIndex_Host int = iota
	dbTblColIndex_Db
	dbTblColIndex_User
	dbTblColIndex_Select_priv
	dbTblColIndex_Insert_priv
	dbTblColIndex_Update_priv
	dbTblColIndex_Delete_priv
	dbTblColIndex_Create_priv
	dbTblColIndex_Drop_priv
	dbTblColIndex_Grant_priv
	dbTblColIndex_References_priv
	dbTblColIndex_Index_priv
	dbTblColIndex_Alter_priv
	dbTblColIndex_Create_tmp_table_priv
	dbTblColIndex_Lock_tables_priv
	dbTblColIndex_Create_view_priv
	dbTblColIndex_Show_view_priv
	dbTblColIndex_Create_routine_priv
	dbTblColIndex_Alter_routine_priv
	dbTblColIndex_Execute_priv
	dbTblColIndex_Event_priv
	dbTblColIndex_Trigger_priv
)
