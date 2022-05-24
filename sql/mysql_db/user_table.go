// Copyright 2021-2022 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"

	"github.com/dolthub/vitess/go/sqltypes"
)

const userTblName = "user"

var (
	errUserPkEntry = fmt.Errorf("the primary key for the `user` table was given an unknown entry")
	errUserPkRow   = fmt.Errorf("the primary key for the `user` table was given a row belonging to an unknown schema")
	errUserSkEntry = fmt.Errorf("the secondary key for the `user` table was given an unknown entry")
	errUserSkRow   = fmt.Errorf("the secondary key for the `user` table was given a row belonging to an unknown schema")

	userTblSchema sql.Schema
)

// UserPrimaryKey is a key that represents the primary key for the "user" Grant Table.
type UserPrimaryKey struct {
	Host string
	User string
}

// UserSecondaryKey is a key that represents the secondary key for the "user" Grant Table, which contains only usernames.
type UserSecondaryKey struct {
	User string
}

var _ in_mem_table.Key = UserPrimaryKey{}
var _ in_mem_table.Key = UserSecondaryKey{}

// KeyFromEntry implements the interface in_mem_table.Key.
func (u UserPrimaryKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	user, ok := entry.(*User)
	if !ok {
		return nil, errUserPkEntry
	}
	return UserPrimaryKey{
		Host: user.Host,
		User: user.User,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (u UserPrimaryKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(userTblSchema) {
		return u, errUserPkRow
	}
	host, ok := row[userTblColIndex_Host].(string)
	if !ok {
		return u, errUserPkRow
	}
	user, ok := row[userTblColIndex_User].(string)
	if !ok {
		return u, errUserPkRow
	}
	return UserPrimaryKey{
		Host: host,
		User: user,
	}, nil
}

// KeyFromEntry implements the interface in_mem_table.Key.
func (u UserSecondaryKey) KeyFromEntry(ctx *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	user, ok := entry.(*User)
	if !ok {
		return nil, errUserSkEntry
	}
	return UserSecondaryKey{
		User: user.User,
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (u UserSecondaryKey) KeyFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(userTblSchema) {
		return u, errUserSkRow
	}
	user, ok := row[userTblColIndex_User].(string)
	if !ok {
		return u, errUserSkRow
	}
	return UserSecondaryKey{
		User: user,
	}, nil
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
	blob_not_null_default_empty := &sql.Column{
		Type:     sql.Blob,
		Default:  mustDefault(expression.NewLiteral("", sql.Blob), sql.Blob, true, false),
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
	text_utf8_bin_nullable_default_empty := &sql.Column{
		Type:     text_utf8_bin,
		Default:  mustDefault(expression.NewLiteral("", text_utf8_bin), text_utf8_bin, true, false),
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
		columnTemplate("ssl_cipher", userTblName, false, blob_not_null_default_empty),
		columnTemplate("x509_issuer", userTblName, false, blob_not_null_default_empty),
		columnTemplate("x509_subject", userTblName, false, blob_not_null_default_empty),
		columnTemplate("max_questions", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("max_updates", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("max_connections", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("max_user_connections", userTblName, false, int_unsigned_not_null_default_0),
		columnTemplate("plugin", userTblName, false, char64_utf8_bin_not_null_default_caching_sha2_password),
		columnTemplate("authentication_string", userTblName, false, text_utf8_bin_nullable_default_empty),
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

func addSuperUser(userTable *mysqlTable, username string, host string, password string) {
	err := userTable.data.Put(sql.NewEmptyContext(), &User{
		User:                username,
		Host:                host,
		PrivilegeSet:        newPrivilegeSetWithAllPrivileges(),
		Plugin:              "mysql_native_password",
		Password:            password,
		PasswordLastChanged: time.Unix(1, 0).UTC(),
		Locked:              false,
		Attributes:          nil,
		IsRole:              false,
	})
	if err != nil {
		panic(err) // Insertion should never fail so this should never be reached
	}
}

// These represent the column indexes of the "user" Grant Table.
const (
	userTblColIndex_Host int = iota
	userTblColIndex_User
	userTblColIndex_Select_priv
	userTblColIndex_Insert_priv
	userTblColIndex_Update_priv
	userTblColIndex_Delete_priv
	userTblColIndex_Create_priv
	userTblColIndex_Drop_priv
	userTblColIndex_Reload_priv
	userTblColIndex_Shutdown_priv
	userTblColIndex_Process_priv
	userTblColIndex_File_priv
	userTblColIndex_Grant_priv
	userTblColIndex_References_priv
	userTblColIndex_Index_priv
	userTblColIndex_Alter_priv
	userTblColIndex_Show_db_priv
	userTblColIndex_Super_priv
	userTblColIndex_Create_tmp_table_priv
	userTblColIndex_Lock_tables_priv
	userTblColIndex_Execute_priv
	userTblColIndex_Repl_slave_priv
	userTblColIndex_Repl_client_priv
	userTblColIndex_Create_view_priv
	userTblColIndex_Show_view_priv
	userTblColIndex_Create_routine_priv
	userTblColIndex_Alter_routine_priv
	userTblColIndex_Create_user_priv
	userTblColIndex_Event_priv
	userTblColIndex_Trigger_priv
	userTblColIndex_Create_tablespace_priv
	userTblColIndex_ssl_type
	userTblColIndex_ssl_cipher
	userTblColIndex_x509_issuer
	userTblColIndex_x509_subject
	userTblColIndex_max_questions
	userTblColIndex_max_updates
	userTblColIndex_max_connections
	userTblColIndex_max_user_connections
	userTblColIndex_plugin
	userTblColIndex_authentication_string
	userTblColIndex_password_expired
	userTblColIndex_password_last_changed
	userTblColIndex_password_lifetime
	userTblColIndex_account_locked
	userTblColIndex_Create_role_priv
	userTblColIndex_Drop_role_priv
	userTblColIndex_Password_reuse_history
	userTblColIndex_Password_reuse_time
	userTblColIndex_Password_require_current
	userTblColIndex_User_attributes
)
