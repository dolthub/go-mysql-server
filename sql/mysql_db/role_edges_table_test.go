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

func TestRoleEdgesTableSchema(t *testing.T) {
	// Each column has a constant index that it expects to match, therefore if a column's position is updated and the
	// variable referencing it hasn't also been updated, this will throw a panic.
	for i, col := range roleEdgesTblSchema {
		switch col.Name {
		case "FROM_HOST":
			if roleEdgesTblColIndex_FROM_HOST != i {
				t.FailNow()
			}
		case "FROM_USER":
			if roleEdgesTblColIndex_FROM_USER != i {
				t.FailNow()
			}
		case "TO_HOST":
			if roleEdgesTblColIndex_TO_HOST != i {
				t.FailNow()
			}
		case "TO_USER":
			if roleEdgesTblColIndex_TO_USER != i {
				t.FailNow()
			}
		case "WITH_ADMIN_OPTION":
			if roleEdgesTblColIndex_WITH_ADMIN_OPTION != i {
				t.FailNow()
			}
		default:
			t.Errorf(`col "%s" does not have a constant`, col.Name)
		}
	}
}
