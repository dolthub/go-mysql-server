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

func TestTablesPrivTableSchema(t *testing.T) {
	// Each column has a constant index that it expects to match, therefore if a column's position is updated and the
	// variable referencing it hasn't also been updated, this will throw a panic.
	for i, col := range tablesPrivTblSchema {
		switch col.Name {
		case "Host":
			if tablesPrivTblColIndex_Host != i {
				t.FailNow()
			}
		case "Db":
			if tablesPrivTblColIndex_Db != i {
				t.FailNow()
			}
		case "User":
			if tablesPrivTblColIndex_User != i {
				t.FailNow()
			}
		case "Table_name":
			if tablesPrivTblColIndex_Table_name != i {
				t.FailNow()
			}
		case "Grantor":
			if tablesPrivTblColIndex_Grantor != i {
				t.FailNow()
			}
		case "Timestamp":
			if tablesPrivTblColIndex_Timestamp != i {
				t.FailNow()
			}
		case "Table_priv":
			if tablesPrivTblColIndex_Table_priv != i {
				t.FailNow()
			}
		case "Column_priv":
			if tablesPrivTblColIndex_Column_priv != i {
				t.FailNow()
			}
		default:
			t.Errorf(`col "%s" does not have a constant`, col.Name)
		}
	}
}
