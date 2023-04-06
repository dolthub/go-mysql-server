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

package plan

import (
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

var _ sql.Node = (*Event)(nil)
var _ sql.DebugStringer = (*Event)(nil)
var _ RepresentsBlock = (*Event)(nil)

// Event is an event that is executed during server run-time only.
type Event struct {
	SchemaName           string
	Name                 string
	Definer              string
	Definition           sql.Node
	ExecuteAt            time.Time
	HasExecuteAt         bool
	Every                *sql.EventOnScheduleEveryInterval
	Starts               time.Time
	HasStarts            bool
	Ends                 time.Time
	HasEnds              bool
	Status               sql.EventStatus
	OnCompletionPreserve bool
	Created              time.Time
	LastAltered          time.Time
	LastExecuted         time.Time
	ExecutionCount       uint64
	Comment              string
	CreateStatement      string

	// TODO: add TimeZone
}

// NewEvent returns Event with only some members defined.
// TimeZone, On Schedule metadata, Created, LastAltered,
// LastExecuted and ExecutionCount needs to be defined explicitly.
func NewEvent(
	schema, name, definer string,
	ocp bool,
	status sql.EventStatus,
	definition sql.Node,
	comment, createString string) *Event {
	return &Event{
		SchemaName:           schema,
		Name:                 name,
		Definer:              definer,
		OnCompletionPreserve: ocp,
		Status:               status,
		Definition:           definition,
		Comment:              comment,
		CreateStatement:      createString,
	}
}

func (e *Event) Resolved() bool {
	return e.Definition.Resolved()
}

func (e *Event) String() string {
	return e.Definition.String()
}

func (e *Event) Schema() sql.Schema {
	return e.Definition.Schema()
}

func (e *Event) Children() []sql.Node {
	return []sql.Node{e.Definition}
}

func (e *Event) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	// TODO: is this called by 'execute event'?
	return e.Definition.RowIter(ctx, row)
}

func (e *Event) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(e, len(children), 1)
	}

	ne := *e
	ne.Definition = children[0]
	return &ne, nil
}

func (e *Event) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return e.Definition.CheckPrivileges(ctx, opChecker)
}

func (e *Event) DebugString() string {
	return sql.DebugString(e.Definition)
}

func (e *Event) implementsRepresentsBlock() {}
