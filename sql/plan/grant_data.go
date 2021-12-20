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

package plan

import (
	"fmt"
	"strings"
)

// Privilege specifies a privilege to be used in a GRANT or REVOKE statement.
type Privilege struct {
	Type    PrivilegeType
	Columns []string
}

// PrivilegeLevel defines the level that a privilege applies to.
type PrivilegeLevel struct {
	Database     string
	TableRoutine string
}

// PrivilegeType is the type of privilege that is being granted or revoked.
type PrivilegeType byte

const (
	PrivilegeType_All PrivilegeType = iota
	PrivilegeType_Insert
	PrivilegeType_References
	PrivilegeType_Select
	PrivilegeType_Update
)

// ObjectType represents the object type that the GRANT or REVOKE statement will apply to.
type ObjectType byte

const (
	ObjectType_Any ObjectType = iota
	ObjectType_Table
	ObjectType_Function
	ObjectType_Procedure
)

// GrantUserAssumptionType is the assumption type that the user executing the GRANT statement will use.
type GrantUserAssumptionType byte

const (
	GrantUserAssumptionType_Default GrantUserAssumptionType = iota
	GrantUserAssumptionType_None
	GrantUserAssumptionType_All
	GrantUserAssumptionType_AllExcept
	GrantUserAssumptionType_Roles
)

// GrantUserAssumption represents the target user that the user executing the GRANT statement will assume the identity of.
type GrantUserAssumption struct {
	Type  GrantUserAssumptionType
	User  UserName
	Roles []UserName
}

// String returns the Privilege as a formatted string.
func (p *Privilege) String() string {
	sb := strings.Builder{}
	switch p.Type {
	case PrivilegeType_All:
		sb.WriteString("ALL")
	case PrivilegeType_Insert:
		sb.WriteString("INSERT")
	case PrivilegeType_References:
		sb.WriteString("REFERENCES")
	case PrivilegeType_Select:
		sb.WriteString("SELECT")
	case PrivilegeType_Update:
		sb.WriteString("UPDATE")
	}
	if len(p.Columns) > 0 {
		sb.WriteString(" (")
		for i, col := range p.Columns {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(col)
		}
		sb.WriteString(")")
	}
	return sb.String()
}

// String returns the PrivilegeLevel as a formatted string.
func (p *PrivilegeLevel) String() string {
	if p.Database == "" {
		if p.TableRoutine == "*" {
			return "*"
		} else {
			return fmt.Sprintf("%s", p.TableRoutine)
		}
	} else if p.Database == "*" {
		return "*.*"
	} else if p.TableRoutine == "*" {
		return fmt.Sprintf("%s.*", p.Database)
	} else {
		return fmt.Sprintf("%s.%s", p.Database, p.TableRoutine)
	}
}
