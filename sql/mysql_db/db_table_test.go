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

import "testing"

func TestDbTableSchema(t *testing.T) {
	// Each column has a constant index that it expects to match, therefore if a column's position is updated and the
	// variable referencing it hasn't also been updated, this will throw a panic.
	for i, col := range dbTblSchema {
		switch col.Name {
		case "Host":
			if dbTblColIndex_Host != i {
				t.FailNow()
			}
		case "Db":
			if dbTblColIndex_Db != i {
				t.FailNow()
			}
		case "User":
			if dbTblColIndex_User != i {
				t.FailNow()
			}
		case "Select_priv":
			if dbTblColIndex_Select_priv != i {
				t.FailNow()
			}
		case "Insert_priv":
			if dbTblColIndex_Insert_priv != i {
				t.FailNow()
			}
		case "Update_priv":
			if dbTblColIndex_Update_priv != i {
				t.FailNow()
			}
		case "Delete_priv":
			if dbTblColIndex_Delete_priv != i {
				t.FailNow()
			}
		case "Create_priv":
			if dbTblColIndex_Create_priv != i {
				t.FailNow()
			}
		case "Drop_priv":
			if dbTblColIndex_Drop_priv != i {
				t.FailNow()
			}
		case "Grant_priv":
			if dbTblColIndex_Grant_priv != i {
				t.FailNow()
			}
		case "References_priv":
			if dbTblColIndex_References_priv != i {
				t.FailNow()
			}
		case "Index_priv":
			if dbTblColIndex_Index_priv != i {
				t.FailNow()
			}
		case "Alter_priv":
			if dbTblColIndex_Alter_priv != i {
				t.FailNow()
			}
		case "Create_tmp_table_priv":
			if dbTblColIndex_Create_tmp_table_priv != i {
				t.FailNow()
			}
		case "Lock_tables_priv":
			if dbTblColIndex_Lock_tables_priv != i {
				t.FailNow()
			}
		case "Create_view_priv":
			if dbTblColIndex_Create_view_priv != i {
				t.FailNow()
			}
		case "Show_view_priv":
			if dbTblColIndex_Show_view_priv != i {
				t.FailNow()
			}
		case "Create_routine_priv":
			if dbTblColIndex_Create_routine_priv != i {
				t.FailNow()
			}
		case "Alter_routine_priv":
			if dbTblColIndex_Alter_routine_priv != i {
				t.FailNow()
			}
		case "Execute_priv":
			if dbTblColIndex_Execute_priv != i {
				t.FailNow()
			}
		case "Event_priv":
			if dbTblColIndex_Event_priv != i {
				t.FailNow()
			}
		case "Trigger_priv":
			if dbTblColIndex_Trigger_priv != i {
				t.FailNow()
			}
		default:
			t.Errorf(`col "%s" does not have a constant`, col.Name)
		}
	}
}
