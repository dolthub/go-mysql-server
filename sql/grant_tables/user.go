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

package grant_tables

import (
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"

	"github.com/dolthub/vitess/go/sqltypes"
)

const userTblName = "user"

var (
	userPkCols      = []uint16{0, 1}
	errUserPkAssign = fmt.Errorf("the primary key for the `user` table expects a host and user string")

	userTblSchema sql.Schema
)

// UserPrimaryKey is a key that represents the primary key for the "user" Grant Table.
type UserPrimaryKey struct {
	Host string
	User string
}

var _ in_mem_table.InMemTableDataKey = UserPrimaryKey{}

// AssignValues implements the interface in_mem_table.InMemTableDataKey.
func (u UserPrimaryKey) AssignValues(vals ...interface{}) (in_mem_table.InMemTableDataKey, error) {
	if len(vals) != 2 {
		return u, errUserPkAssign
	}
	host, ok := vals[0].(string)
	if !ok {
		return u, errUserPkAssign
	}
	user, ok := vals[1].(string)
	if !ok {
		return u, errUserPkAssign
	}
	return UserPrimaryKey{
		Host: host,
		User: user,
	}, nil
}

// RepresentedColumns implements the interface in_mem_table.InMemTableDataKey.
func (u UserPrimaryKey) RepresentedColumns() []uint16 {
	return userPkCols
}

// init creates the schema for the "user" Grant Table.
func init() {
	// Types
	char32_utf8_bin := sql.MustCreateString(sqltypes.Char, 32, sql.Collation_utf8_bin)
	char64_utf8_bin := sql.MustCreateString(sqltypes.Char, 64, sql.Collation_utf8_bin)
	char255_ascii_general_ci := sql.MustCreateString(sqltypes.Char, 255, sql.Collation_ascii_general_ci)
	enum_ANY_X509_SPECIFIED_utf8_general_ci := sql.MustCreateEnumType([]string{"", "ANY", "X509", "SPECIFIED"}, sql.Collation_utf8_general_ci)
	enum_N_Y_utf8_general_ci := sql.MustCreateEnumType([]string{"N", "Y"}, sql.Collation_utf8_general_ci)
	text_utf8_bin := sql.CreateText(sql.Collation_utf8_bin)

	// Column Templates
	blob_not_null_default_nil := &sql.Column{
		Type:     sql.Blob,
		Default:  nil,
		Nullable: false,
	}
	char32_utf8_bin_not_null_default_empty := &sql.Column{
		Type:     char32_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", char32_utf8_bin), char32_utf8_bin, true, false),
		Nullable: false,
	}
	char64_utf8_bin_not_null_default_caching_sha2_password := &sql.Column{
		Type:     char64_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("caching_sha2_password", char64_utf8_bin), char64_utf8_bin, true, false),
		Nullable: false,
	}
	char255_ascii_general_ci_not_null_default_empty := &sql.Column{
		Type:     char255_ascii_general_ci,
		Default:  mustDefault(expression.NewLiteral("", char255_ascii_general_ci), char255_ascii_general_ci, true, false),
		Nullable: false,
	}
	enum_ANY_X509_SPECIFIED_utf8_general_ci_not_null_default_empty := &sql.Column{
		Type:     enum_ANY_X509_SPECIFIED_utf8_general_ci,
		Default:  mustDefault(expression.NewLiteral("", enum_ANY_X509_SPECIFIED_utf8_general_ci), enum_ANY_X509_SPECIFIED_utf8_general_ci, true, false),
		Nullable: false,
	}
	enum_N_Y_utf8_general_ci_not_null_default_N := &sql.Column{
		Type:     enum_N_Y_utf8_general_ci,
		Default:  mustDefault(expression.NewLiteral("N", enum_N_Y_utf8_general_ci), enum_N_Y_utf8_general_ci, true, false),
		Nullable: false,
	}
	enum_N_Y_utf8_general_ci_nullable_default_nil := &sql.Column{
		Type:     enum_N_Y_utf8_general_ci,
		Default:  nil,
		Nullable: true,
	}
	int_unsigned_not_null_default_0 := &sql.Column{
		Type:     sql.Uint32,
		Default:  mustDefault(expression.NewLiteral(uint32(0), sql.Uint32), sql.Uint32, true, false),
		Nullable: false,
	}
	json_nullable_default_nil := &sql.Column{
		Type:     sql.JSON,
		Default:  nil,
		Nullable: true,
	}
	smallint_unsigned_nullable_default_nil := &sql.Column{
		Type:     sql.Uint16,
		Default:  nil,
		Nullable: true,
	}
	text_utf8_bin_nullable_default_nil := &sql.Column{
		Type:     text_utf8_bin,
		Default:  nil,
		Nullable: true,
	}
	timestamp_nullable_default_nil := &sql.Column{
		Type:     sql.Timestamp,
		Default:  nil,
		Nullable: true,
	}

	userTblSchema = sql.Schema{
		columnTemplate("Host", userTblName, true, char255_ascii_general_ci_not_null_default_empty),
		columnTemplate("User", userTblName, true, char32_utf8_bin_not_null_default_empty),
		columnTemplate("Select_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Insert_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Update_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Delete_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Drop_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Reload_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Shutdown_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Process_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("File_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Grant_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("References_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Index_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Alter_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Show_db_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Super_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_tmp_table_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Lock_tables_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Execute_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Repl_slave_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Repl_client_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_view_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Show_view_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_routine_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Alter_routine_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_user_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Event_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Trigger_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_tablespace_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("ssl_type", userTblName, false, enum_ANY_X509_SPECIFIED_utf8_general_ci_not_null_default_empty),
		columnTemplate("ssl_cipher", userTblName, false, blob_not_null_default_nil),
		columnTemplate("x509_issuer", userTblName, false, blob_not_null_default_nil),
		columnTemplate("x509_subject", userTblName, false, blob_not_null_default_nil),
		columnTemplate("max_questions", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("max_updates", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("max_connections", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("max_user_connections", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("plugin", userTblName, false, char64_utf8_bin_not_null_default_caching_sha2_password),
		columnTemplate("authentication_string", userTblName, false, text_utf8_bin_nullable_default_nil),
		columnTemplate("password_expired", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("password_last_changed", userTblName, false, timestamp_nullable_default_nil),
		columnTemplate("password_lifetime", userTblName, false, smallint_unsigned_nullable_default_nil),
		columnTemplate("account_locked", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Create_role_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Drop_role_priv", userTblName, false, enum_N_Y_utf8_general_ci_not_null_default_N),
		columnTemplate("Password_reuse_history", userTblName, false, smallint_unsigned_nullable_default_nil),
		columnTemplate("Password_reuse_time", userTblName, false, smallint_unsigned_nullable_default_nil),
		columnTemplate("Password_require_current", userTblName, false, enum_N_Y_utf8_general_ci_nullable_default_nil),
		columnTemplate("User_attributes", userTblName, false, json_nullable_default_nil),
	}
}

func addDefaultRootUser(userTable *grantTable) {
	err := userTable.Data().Put(sql.Row{
		"localhost",             // 00: Host
		"root",                  // 01: User
		"Y",                     // 02: Select_priv
		"Y",                     // 03: Insert_priv
		"Y",                     // 04: Update_priv
		"Y",                     // 05: Delete_priv
		"Y",                     // 06: Create_priv
		"Y",                     // 07: Drop_priv
		"Y",                     // 08: Reload_priv
		"Y",                     // 09: Shutdown_priv
		"Y",                     // 10: Process_priv
		"Y",                     // 11: File_priv
		"Y",                     // 12: Grant_priv
		"Y",                     // 13: References_priv
		"Y",                     // 14: Index_priv
		"Y",                     // 15: Alter_priv
		"Y",                     // 16: Show_db_priv
		"Y",                     // 17: Super_priv
		"Y",                     // 18: Create_tmp_table_priv
		"Y",                     // 19: Lock_tables_priv
		"Y",                     // 20: Execute_priv
		"Y",                     // 21: Repl_slave_priv
		"Y",                     // 22: Repl_client_priv
		"Y",                     // 23: Create_view_priv
		"Y",                     // 24: Show_view_priv
		"Y",                     // 25: Create_routine_priv
		"Y",                     // 26: Alter_routine_priv
		"Y",                     // 27: Create_user_priv
		"Y",                     // 28: Event_priv
		"Y",                     // 29: Trigger_priv
		"Y",                     // 30: Create_tablespace_priv
		"",                      // 31: ssl_type
		"",                      // 32: ssl_cipher
		"",                      // 33: x509_issuer
		"",                      // 34: x509_subject
		0,                       // 35: max_questions
		0,                       // 36: max_updates
		0,                       // 37: max_connections
		0,                       // 38: max_user_connections
		"caching_sha2_password", // 39: plugin
		nil,                     // 40: authentication_string //TODO: figure out what this password should be
		"N",                     // 41: password_expired
		time.Unix(0, 0).UTC(),   // 42: password_last_changed
		nil,                     // 43: password_lifetime
		"N",                     // 44: account_locked
		"Y",                     // 45: Create_role_priv
		"Y",                     // 46: Drop_role_priv
		nil,                     // 47: Password_reuse_history
		nil,                     // 48: Password_reuse_time
		nil,                     // 49: Password_require_current
		nil,                     // 50: User_attributes
	})
	if err != nil {
		panic(err) // Insertion should never fail so this should never be reached
	}
}
