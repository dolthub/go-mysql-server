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
	"fmt"
	"strings"

	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/gabereiser/go-mysql-server/sql"
	"github.com/gabereiser/go-mysql-server/sql/expression"
	"github.com/gabereiser/go-mysql-server/sql/in_mem_table"
	"github.com/gabereiser/go-mysql-server/sql/types"
)

const globalGrantsTblName = "global_grants"

var (
	errGlobalGrantEntry = fmt.Errorf("the converter for the `global_grants` table was given an unknown entry")
	errGlobalGrantRow   = fmt.Errorf("the converter for the `global_grants` table was given a row belonging to an unknown schema")

	globalGrantsTblSchema sql.Schema
)

// GlobalGrantsConverter handles the conversion between a stored *User entry and the faux "global_grants" Grant Table.
type GlobalGrantsConverter struct{}

var _ in_mem_table.DataEditorConverter = GlobalGrantsConverter{}

// RowToKey implements the interface in_mem_table.DataEditorConverter.
func (conv GlobalGrantsConverter) RowToKey(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(globalGrantsTblSchema) {
		return nil, errGlobalGrantRow
	}
	host, ok := row[globalGrantsTblColIndex_HOST].(string)
	if !ok {
		return nil, errGlobalGrantRow
	}
	user, ok := row[globalGrantsTblColIndex_USER].(string)
	if !ok {
		return nil, errGlobalGrantRow
	}
	return UserPrimaryKey{
		Host: host,
		User: user,
	}, nil
}

// AddRowToEntry implements the interface in_mem_table.DataEditorConverter.
func (conv GlobalGrantsConverter) AddRowToEntry(ctx *sql.Context, row sql.Row, entry in_mem_table.Entry) (in_mem_table.Entry, error) {
	if len(row) != len(globalGrantsTblSchema) {
		return nil, errGlobalGrantRow
	}
	user, ok := entry.(*User)
	if !ok {
		return nil, errGlobalGrantEntry
	}
	user = user.Copy(ctx).(*User)

	privilege, ok := row[globalGrantsTblColIndex_PRIV].(string)
	if !ok {
		return nil, errGlobalGrantRow
	}
	withGrantOption, ok := row[globalGrantsTblColIndex_WITH_GRANT_OPTION].(uint16)
	if !ok {
		return nil, errGlobalGrantRow
	}
	// A value of 1 is equivalent to 'N', a value of 2 is equivalent to 'Y'
	user.PrivilegeSet.AddGlobalDynamic(withGrantOption == 2, privilege)
	return user, nil
}

// RemoveRowFromEntry implements the interface in_mem_table.DataEditorConverter.
func (conv GlobalGrantsConverter) RemoveRowFromEntry(ctx *sql.Context, row sql.Row, entry in_mem_table.Entry) (in_mem_table.Entry, error) {
	if len(row) != len(globalGrantsTblSchema) {
		return nil, errGlobalGrantRow
	}
	user, ok := entry.(*User)
	if !ok {
		return nil, errGlobalGrantEntry
	}
	user = user.Copy(ctx).(*User)

	privilege, ok := row[globalGrantsTblColIndex_PRIV].(string)
	if !ok {
		return nil, errGlobalGrantRow
	}
	//TODO: handle "WITH GRANT OPTION"
	//withGrantOption, ok := row[globalGrantsTblColIndex_WITH_GRANT_OPTION].(uint16)
	//if !ok {
	//	return nil, errGlobalGrantRow
	//}
	user.PrivilegeSet.RemoveGlobalDynamic(privilege)
	return user, nil
}

// EntryToRows implements the interface in_mem_table.DataEditorConverter.
func (conv GlobalGrantsConverter) EntryToRows(ctx *sql.Context, entry in_mem_table.Entry) ([]sql.Row, error) {
	user, ok := entry.(*User)
	if !ok {
		return nil, errGlobalGrantEntry
	}

	var rows []sql.Row
	for dynamicPriv, _ := range user.PrivilegeSet.globalDynamic {
		row := make(sql.Row, len(globalGrantsTblSchema))
		var err error
		for i, col := range globalGrantsTblSchema {
			row[i], err = col.Default.Eval(ctx, nil)
			if err != nil {
				return nil, err // Should never happen, schema is static
			}
		}

		row[globalGrantsTblColIndex_USER] = user.User
		row[globalGrantsTblColIndex_HOST] = user.Host
		row[globalGrantsTblColIndex_PRIV] = strings.ToUpper(dynamicPriv)
		//TODO: handle "WITH GRANT OPTION"
		row[globalGrantsTblColIndex_WITH_GRANT_OPTION] = 2
	}

	return rows, nil
}

// init creates the schema for the "global_grants" Grant Table.
func init() {
	// Types
	char32_utf8_bin := types.MustCreateString(sqltypes.Char, 32, sql.Collation_utf8_bin)
	char32_utf8_general_ci := types.MustCreateString(sqltypes.Char, 32, sql.Collation_utf8_general_ci)
	char255_ascii_general_ci := types.MustCreateString(sqltypes.Char, 255, sql.Collation_ascii_general_ci)
	enum_N_Y_utf8_general_ci := types.MustCreateEnumType([]string{"N", "Y"}, sql.Collation_utf8_general_ci)

	// Column Templates
	char32_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char32_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char32_utf8_bin), char32_utf8_bin, true, false),
		Nullable: false,
	}
	char32_utf8_general_ci_not_null_default_empty := &sql.Column{
		Type:     char32_utf8_general_ci,
		Default:  mustDefault(expression.NewLiteral("", char32_utf8_general_ci), char32_utf8_general_ci, true, false),
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

	globalGrantsTblSchema = sql.Schema{
		columnTemplate("USER", globalGrantsTblName, true, char32_utf8_bin_not_null_default_empty),
		columnTemplate("HOST", globalGrantsTblName, true, char255_ascii_general_ci_not_null_default_empty),
		columnTemplate("PRIV", globalGrantsTblName, true, char32_utf8_general_ci_not_null_default_empty),
		columnTemplate("WITH_GRANT_OPTION", globalGrantsTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
	}
}

// These represent the column indexes of the "global_grants" Grant Table.
const (
	globalGrantsTblColIndex_USER int = iota
	globalGrantsTblColIndex_HOST
	globalGrantsTblColIndex_PRIV
	globalGrantsTblColIndex_WITH_GRANT_OPTION
)
