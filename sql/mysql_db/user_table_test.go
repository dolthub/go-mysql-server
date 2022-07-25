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

package mysql_db

import (
	"testing"
)

func TestUserTableSchema(t *testing.T) {
	// Each column has a constant index that it expects to match, therefore if a column's position is updated and the
	// variable referencing it hasn't also been updated, this will throw a panic.
	for i, col := range userTblSchema {
		switch col.Name {
		case "Host":
			if userTblColIndex_Host != i {
				t.FailNow()
			}
		case "User":
			if userTblColIndex_User != i {
				t.FailNow()
			}
		case "Select_priv":
			if userTblColIndex_Select_priv != i {
				t.FailNow()
			}
		case "Insert_priv":
			if userTblColIndex_Insert_priv != i {
				t.FailNow()
			}
		case "Update_priv":
			if userTblColIndex_Update_priv != i {
				t.FailNow()
			}
		case "Delete_priv":
			if userTblColIndex_Delete_priv != i {
				t.FailNow()
			}
		case "Create_priv":
			if userTblColIndex_Create_priv != i {
				t.FailNow()
			}
		case "Drop_priv":
			if userTblColIndex_Drop_priv != i {
				t.FailNow()
			}
		case "Reload_priv":
			if userTblColIndex_Reload_priv != i {
				t.FailNow()
			}
		case "Shutdown_priv":
			if userTblColIndex_Shutdown_priv != i {
				t.FailNow()
			}
		case "Process_priv":
			if userTblColIndex_Process_priv != i {
				t.FailNow()
			}
		case "File_priv":
			if userTblColIndex_File_priv != i {
				t.FailNow()
			}
		case "Grant_priv":
			if userTblColIndex_Grant_priv != i {
				t.FailNow()
			}
		case "References_priv":
			if userTblColIndex_References_priv != i {
				t.FailNow()
			}
		case "Index_priv":
			if userTblColIndex_Index_priv != i {
				t.FailNow()
			}
		case "Alter_priv":
			if userTblColIndex_Alter_priv != i {
				t.FailNow()
			}
		case "Show_db_priv":
			if userTblColIndex_Show_db_priv != i {
				t.FailNow()
			}
		case "Super_priv":
			if userTblColIndex_Super_priv != i {
				t.FailNow()
			}
		case "Create_tmp_table_priv":
			if userTblColIndex_Create_tmp_table_priv != i {
				t.FailNow()
			}
		case "Lock_tables_priv":
			if userTblColIndex_Lock_tables_priv != i {
				t.FailNow()
			}
		case "Execute_priv":
			if userTblColIndex_Execute_priv != i {
				t.FailNow()
			}
		case "Repl_slave_priv":
			if userTblColIndex_Repl_slave_priv != i {
				t.FailNow()
			}
		case "Repl_client_priv":
			if userTblColIndex_Repl_client_priv != i {
				t.FailNow()
			}
		case "Create_view_priv":
			if userTblColIndex_Create_view_priv != i {
				t.FailNow()
			}
		case "Show_view_priv":
			if userTblColIndex_Show_view_priv != i {
				t.FailNow()
			}
		case "Create_routine_priv":
			if userTblColIndex_Create_routine_priv != i {
				t.FailNow()
			}
		case "Alter_routine_priv":
			if userTblColIndex_Alter_routine_priv != i {
				t.FailNow()
			}
		case "Create_user_priv":
			if userTblColIndex_Create_user_priv != i {
				t.FailNow()
			}
		case "Event_priv":
			if userTblColIndex_Event_priv != i {
				t.FailNow()
			}
		case "Trigger_priv":
			if userTblColIndex_Trigger_priv != i {
				t.FailNow()
			}
		case "Create_tablespace_priv":
			if userTblColIndex_Create_tablespace_priv != i {
				t.FailNow()
			}
		case "ssl_type":
			if userTblColIndex_ssl_type != i {
				t.FailNow()
			}
		case "ssl_cipher":
			if userTblColIndex_ssl_cipher != i {
				t.FailNow()
			}
		case "x509_issuer":
			if userTblColIndex_x509_issuer != i {
				t.FailNow()
			}
		case "x509_subject":
			if userTblColIndex_x509_subject != i {
				t.FailNow()
			}
		case "max_questions":
			if userTblColIndex_max_questions != i {
				t.FailNow()
			}
		case "max_updates":
			if userTblColIndex_max_updates != i {
				t.FailNow()
			}
		case "max_connections":
			if userTblColIndex_max_connections != i {
				t.FailNow()
			}
		case "max_user_connections":
			if userTblColIndex_max_user_connections != i {
				t.FailNow()
			}
		case "plugin":
			if userTblColIndex_plugin != i {
				t.FailNow()
			}
		case "authentication_string":
			if userTblColIndex_authentication_string != i {
				t.FailNow()
			}
		case "password_expired":
			if userTblColIndex_password_expired != i {
				t.FailNow()
			}
		case "password_last_changed":
			if userTblColIndex_password_last_changed != i {
				t.FailNow()
			}
		case "password_lifetime":
			if userTblColIndex_password_lifetime != i {
				t.FailNow()
			}
		case "account_locked":
			if userTblColIndex_account_locked != i {
				t.FailNow()
			}
		case "Create_role_priv":
			if userTblColIndex_Create_role_priv != i {
				t.FailNow()
			}
		case "Drop_role_priv":
			if userTblColIndex_Drop_role_priv != i {
				t.FailNow()
			}
		case "Password_reuse_history":
			if userTblColIndex_Password_reuse_history != i {
				t.FailNow()
			}
		case "Password_reuse_time":
			if userTblColIndex_Password_reuse_time != i {
				t.FailNow()
			}
		case "Password_require_current":
			if userTblColIndex_Password_require_current != i {
				t.FailNow()
			}
		case "User_attributes":
			if userTblColIndex_User_attributes != i {
				t.FailNow()
			}
		case "identity":
			if userTblColIndex_identity != i {
				t.FailNow()
			}
		default:
			t.Errorf(`col "%s" does not have a constant`, col.Name)
		}
	}
}
