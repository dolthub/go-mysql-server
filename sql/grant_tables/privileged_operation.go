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

package grant_tables

// Operation represents an operation that requires privileges to execute.
type Operation struct {
	Database   string
	Table      string
	Column     string
	Privileges []PrivilegeType
}

func NewOperation(dbName string, tblName string, colName string, privs ...PrivilegeType) Operation {
	return Operation{
		Database:   dbName,
		Table:      tblName,
		Column:     colName,
		Privileges: privs,
	}
}
