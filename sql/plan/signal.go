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

	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/go-mysql-server/sql"
)

// SignalConditionItemName represents the item name for the set conditions of a SIGNAL statement.
type SignalConditionItemName string

const (
	SignalConditionItemName_ClassOrigin       SignalConditionItemName = "class_origin"
	SignalConditionItemName_SubclassOrigin    SignalConditionItemName = "subclass_origin"
	SignalConditionItemName_MessageText       SignalConditionItemName = "message_text"
	SignalConditionItemName_MysqlErrno        SignalConditionItemName = "mysql_errno"
	SignalConditionItemName_ConstraintCatalog SignalConditionItemName = "constraint_catalog"
	SignalConditionItemName_ConstraintSchema  SignalConditionItemName = "constraint_schema"
	SignalConditionItemName_ConstraintName    SignalConditionItemName = "constraint_name"
	SignalConditionItemName_CatalogName       SignalConditionItemName = "catalog_name"
	SignalConditionItemName_SchemaName        SignalConditionItemName = "schema_name"
	SignalConditionItemName_TableName         SignalConditionItemName = "table_name"
	SignalConditionItemName_ColumnName        SignalConditionItemName = "column_name"
	SignalConditionItemName_CursorName        SignalConditionItemName = "cursor_name"
)

// SignalInfo represents a piece of information for a SIGNAL statement.
type SignalInfo struct {
	ConditionItemName SignalConditionItemName
	IntValue          int64
	StrValue          string
}

// Signal represents the SIGNAL statement with a set SQLSTATE.
type Signal struct {
	SqlStateValue string // Will always be a string with length 5
	Info          map[SignalConditionItemName]SignalInfo
}

// SignalName represents the SIGNAL statement with a condition name.
type SignalName struct {
	Signal *Signal
	Name   string
}

var _ sql.Node = (*Signal)(nil)
var _ sql.Node = (*SignalName)(nil)

// NewSignal returns a *Signal node.
func NewSignal(sqlstate string, info map[SignalConditionItemName]SignalInfo) *Signal {
	// https://dev.mysql.com/doc/refman/8.0/en/signal.html#signal-condition-information-items
	// https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
	firstTwo := sqlstate[0:2]
	if _, ok := info[SignalConditionItemName_MessageText]; !ok {
		si := SignalInfo{
			ConditionItemName: SignalConditionItemName_MessageText,
		}
		switch firstTwo {
		case "01":
			si.StrValue = "Unhandled user-defined warning condition"
		case "02":
			si.StrValue = "Unhandled user-defined not found condition"
		default:
			si.StrValue = "Unhandled user-defined exception condition"
		}
		info[SignalConditionItemName_MessageText] = si
	}
	if _, ok := info[SignalConditionItemName_MysqlErrno]; !ok {
		si := SignalInfo{
			ConditionItemName: SignalConditionItemName_MysqlErrno,
		}
		switch firstTwo {
		case "01":
			si.IntValue = 1642
		case "02":
			si.IntValue = 1643
		default:
			si.IntValue = 1644
		}
		info[SignalConditionItemName_MysqlErrno] = si
	}
	return &Signal{
		SqlStateValue: sqlstate,
		Info:          info,
	}
}

// NewSignalName returns a *SignalName node.
func NewSignalName(name string, info map[SignalConditionItemName]SignalInfo) *SignalName {
	return &SignalName{
		Signal: &Signal{
			Info: info,
		},
		Name: name,
	}
}

// Resolved implements the sql.Node interface.
func (s *Signal) Resolved() bool {
	return true
}

// String implements the sql.Node interface.
func (s *Signal) String() string {
	infoStr := ""
	if len(s.Info) > 0 {
		infoStr = " SET"
		i := 0
		for _, info := range s.Info {
			if i > 0 {
				infoStr += ","
			}
			infoStr += " " + info.String()
			i++
		}
	}
	return fmt.Sprintf("SIGNAL SQLSTATE '%s'%s", s.SqlStateValue, infoStr)
}

// Schema implements the sql.Node interface.
func (s *Signal) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (s *Signal) Children() []sql.Node {
	return nil
}

// WithChildren implements the sql.Node interface.
func (s *Signal) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(s, children...)
}

// RowIter implements the sql.Node interface.
func (s *Signal) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	//TODO: implement CLASS_ORIGIN
	//TODO: implement SUBCLASS_ORIGIN
	//TODO: implement CONSTRAINT_CATALOG
	//TODO: implement CONSTRAINT_SCHEMA
	//TODO: implement CONSTRAINT_NAME
	//TODO: implement CATALOG_NAME
	//TODO: implement SCHEMA_NAME
	//TODO: implement TABLE_NAME
	//TODO: implement COLUMN_NAME
	//TODO: implement CURSOR_NAME
	if s.SqlStateValue[0:2] == "01" {
		//TODO: implement warnings
		return nil, fmt.Errorf("warnings not yet implemented")
	} else {
		return nil, mysql.NewSQLError(
			int(s.Info[SignalConditionItemName_MysqlErrno].IntValue),
			s.SqlStateValue,
			s.Info[SignalConditionItemName_MessageText].StrValue,
		)
	}
}

// Resolved implements the sql.Node interface.
func (s *SignalName) Resolved() bool {
	return true
}

// String implements the sql.Node interface.
func (s *SignalName) String() string {
	infoStr := ""
	if len(s.Signal.Info) > 0 {
		infoStr = " SET"
		i := 0
		for _, info := range s.Signal.Info {
			if i > 0 {
				infoStr += ","
			}
			infoStr += " " + info.String()
			i++
		}
	}
	return fmt.Sprintf("SIGNAL %s%s", s.Name, infoStr)
}

// Schema implements the sql.Node interface.
func (s *SignalName) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (s *SignalName) Children() []sql.Node {
	return nil // SignalName is an alternate form of Signal rather than an encapsulating node, thus no children
}

// WithChildren implements the sql.Node interface.
func (s *SignalName) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(s, children...)
}

// RowIter implements the sql.Node interface.
func (s *SignalName) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	return nil, fmt.Errorf("may not iterate over unresolved node *SignalName")
}

func (s SignalInfo) String() string {
	itemName := strings.ToUpper(string(s.ConditionItemName))
	if s.ConditionItemName == SignalConditionItemName_MysqlErrno {
		return fmt.Sprintf("%s = %d", itemName, s.IntValue)
	}
	return fmt.Sprintf("%s = %s", itemName, s.StrValue)
}
