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
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.Node = (*AlterEvent)(nil)
var _ sql.Expressioner = (*AlterEvent)(nil)
var _ sql.Databaser = (*AlterEvent)(nil)

type AlterEvent struct {
	ddlNode
	EventName string
	Definer   string

	AlterOnSchedule bool
	At              *OnScheduleTimestamp
	Every           *expression.Interval
	Starts          *OnScheduleTimestamp
	Ends            *OnScheduleTimestamp

	AlterOnComp    bool
	OnCompPreserve bool

	AlterName    bool
	RenameToDb   string
	RenameToName string

	AlterStatus bool
	Status      EventStatus

	AlterComment bool
	Comment      string

	AlterDefinition  bool
	DefinitionString string
	DefinitionNode   sql.Node

	// This will be defined during analyzer
	Event sql.EventDetails
}

// NewAlterEvent returns a *AlterEvent node.
func NewAlterEvent(
	db sql.Database,
	name, definer string,
	alterSchedule bool,
	at, starts, ends *OnScheduleTimestamp,
	every *expression.Interval,
	alterOnComp bool,
	onCompletionPreserve bool,
	alterName bool,
	newName string,
	alterStatus bool,
	status EventStatus,
	alterComment bool,
	comment string,
	alterDefinition bool,
	definitionString string,
	definition sql.Node,
) *AlterEvent {
	return &AlterEvent{
		ddlNode:          ddlNode{db},
		EventName:        name,
		Definer:          definer,
		AlterOnSchedule:  alterSchedule,
		At:               at,
		Every:            every,
		Starts:           starts,
		Ends:             ends,
		AlterOnComp:      alterOnComp,
		OnCompPreserve:   onCompletionPreserve,
		AlterName:        alterName,
		RenameToDb:       "", // TODO: moving events across dbs is not supported yet
		RenameToName:     newName,
		AlterStatus:      alterStatus,
		Status:           status,
		AlterComment:     alterComment,
		Comment:          comment,
		AlterDefinition:  alterDefinition,
		DefinitionString: definitionString,
		DefinitionNode:   definition,
	}
}

// String implements the sql.Node interface.
func (a *AlterEvent) String() string {
	stmt := "ALTER"

	if a.Definer != "" {
		stmt = fmt.Sprintf("%s DEFINER = %s", stmt, a.Definer)
	}

	stmt = fmt.Sprintf("%s EVENT", stmt)

	if a.AlterOnSchedule {
		if a.At != nil {
			stmt = fmt.Sprintf("%s ON SCHEDULE AT %s", stmt, a.At.String())
		} else {
			stmt = fmt.Sprintf("%s %s", stmt, onScheduleEveryString(a.Every, a.Starts, a.Ends))
		}
	}

	if a.AlterOnComp {
		onComp := "NOT PRESERVE"
		if a.OnCompPreserve {
			onComp = "PRESERVE"
		}
		stmt = fmt.Sprintf("%s ON COMPLETION %s", stmt, onComp)
	}

	if a.AlterName {
		// rename event database (moving event) is not supported yet
		stmt = fmt.Sprintf("%s RENAMTE TO %s", stmt, a.RenameToName)
	}

	if a.AlterStatus {
		stmt = fmt.Sprintf("%s %s", stmt, a.Status.String())
	}

	if a.AlterComment {
		if a.Comment != "" {
			stmt = fmt.Sprintf("%s COMMENT %s", stmt, a.Comment)
		}
	}

	if a.AlterDefinition {
		stmt = fmt.Sprintf("%s DO %s", stmt, sql.DebugString(a.DefinitionNode))
	}

	return stmt
}

// Resolved implements the sql.Node interface.
func (a *AlterEvent) Resolved() bool {
	r := a.ddlNode.Resolved()

	if a.AlterDefinition {
		r = r && a.DefinitionNode.Resolved()
	}
	if a.AlterOnSchedule {
		if a.At != nil {
			r = r && a.At.Resolved()
		} else {
			r = r && a.Every.Resolved()
			if a.Starts != nil {
				r = r && a.Starts.Resolved()
			}
			if a.Ends != nil {
				r = r && a.Ends.Resolved()
			}
		}
	}
	return r
}

// Schema implements the sql.Node interface.
func (a *AlterEvent) Schema() sql.Schema {
	return nil
}

func (a *AlterEvent) IsReadOnly() bool {
	return false
}

// Children implements the sql.Node interface.
func (a *AlterEvent) Children() []sql.Node {
	if a.AlterDefinition {
		return []sql.Node{a.DefinitionNode}
	}
	return nil
}

// WithChildren implements the sql.Node interface.
func (a *AlterEvent) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) > 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(children), "0 or 1")
	}

	if !a.AlterDefinition {
		return a, nil
	}

	na := *a
	na.DefinitionNode = children[0]
	return &na, nil
}

// CheckPrivileges implements the sql.Node interface.
func (a *AlterEvent) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	hasPriv := opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(a.Db.Name(), "", "", sql.PrivilegeType_Event))

	if a.AlterName && a.RenameToDb != "" {
		hasPriv = hasPriv && opChecker.UserHasPrivileges(ctx,
			sql.NewPrivilegedOperation(a.RenameToDb, "", "", sql.PrivilegeType_Event))
	}
	return hasPriv
}

// Database implements the sql.Databaser interface.
func (a *AlterEvent) Database() sql.Database {
	return a.Db
}

// WithDatabase implements the sql.Databaser interface.
func (a *AlterEvent) WithDatabase(database sql.Database) (sql.Node, error) {
	ae := *a
	ae.Db = database
	return &ae, nil
}

// RowIter implements the sql.Node interface.
func (a *AlterEvent) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	eventDb, ok := a.Db.(sql.EventDatabase)
	if !ok {
		return nil, sql.ErrEventsNotSupported.New(a.Db.Name())
	}

	// sanity check that Event was successfully loaded in analyzer
	if a.Event.Name == "" {
		return nil, fmt.Errorf("error loading existing event to alter from the database")
	}
	var err error
	ed := a.Event
	eventAlteredTime := time.Now()
	ed.LastAltered = eventAlteredTime
	ed.Definer = a.Definer

	if a.AlterOnSchedule {
		if a.At != nil {
			ed.HasExecuteAt = true
			ed.ExecuteAt, err = a.At.EvalTime(ctx)
			if err != nil {
				return nil, err
			}
			// if Schedule was defined using EVERY previously, clear its fields
			ed.ExecuteEvery = ""
			ed.Starts = time.Time{}
			ed.Ends = time.Time{}
			ed.HasEnds = false
		} else {
			delta, err := a.Every.EvalDelta(ctx, nil)
			if err != nil {
				return nil, err
			}
			interval := NewEveryInterval(delta.Years, delta.Months, delta.Days, delta.Hours, delta.Minutes, delta.Seconds)
			iVal, iField := interval.GetIntervalValAndField()
			ed.ExecuteEvery = fmt.Sprintf("%s %s", iVal, iField)

			if a.Starts != nil {
				ed.Starts, err = a.Starts.EvalTime(ctx)
				if err != nil {
					return nil, err
				}
			} else {
				// If STARTS is not defined, it defaults to CURRENT_TIMESTAMP
				ed.Starts = eventAlteredTime
			}
			if a.Ends != nil {
				ed.HasEnds = true
				ed.Ends, err = a.Ends.EvalTime(ctx)
				if err != nil {
					return nil, err
				}
			}
			// if Schedule was defined using AT previously, clear its fields
			ed.HasExecuteAt = false
			ed.ExecuteAt = time.Time{}
		}
	}
	if a.AlterOnComp {
		ed.OnCompletionPreserve = a.OnCompPreserve
	}
	if a.AlterName {
		ed.Name = a.RenameToName
	}
	if a.AlterStatus {
		ed.Status = a.Status.String()
	}
	if a.AlterComment {
		ed.Comment = a.Comment
	}
	if a.AlterDefinition {
		ed.Definition = a.DefinitionString
	}

	return &alterEventIter{
		originalName:  a.EventName,
		alterSchedule: a.AlterOnSchedule,
		alterStatus:   a.AlterStatus,
		eventDetails:  ed,
		eventDb:       eventDb,
	}, nil
}

// Expressions implements the sql.Expressioner interface.
func (a *AlterEvent) Expressions() []sql.Expression {
	if a.AlterOnSchedule {
		if a.At != nil {
			return []sql.Expression{a.At}
		} else {
			if a.Starts == nil && a.Ends == nil {
				return []sql.Expression{a.Every}
			} else if a.Starts == nil {
				return []sql.Expression{a.Every, a.Ends}
			} else if a.Ends == nil {
				return []sql.Expression{a.Every, a.Starts}
			} else {
				return []sql.Expression{a.Every, a.Starts, a.Ends}
			}
		}
	}
	return nil
}

// WithExpressions implements the sql.Expressioner interface.
func (a *AlterEvent) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	if len(e) > 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(e), "up to 3")
	}

	if !a.AlterOnSchedule {
		return a, nil
	}

	na := *a
	if a.At != nil {
		ts, ok := e[0].(*OnScheduleTimestamp)
		if !ok {
			return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", e[0])
		}
		na.At = ts
	} else {
		every, ok := e[0].(*expression.Interval)
		if !ok {
			return nil, fmt.Errorf("expected `*expression.Interval` but got `%T`", e[0])
		}
		na.Every = every

		var ts *OnScheduleTimestamp
		if len(e) > 1 {
			ts, ok = e[1].(*OnScheduleTimestamp)
			if !ok {
				return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", e[1])
			}
			if a.Starts != nil {
				na.Starts = ts
			} else if a.Ends != nil {
				na.Ends = ts
			}
		}

		if len(e) == 3 {
			ts, ok = e[2].(*OnScheduleTimestamp)
			if !ok {
				return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", e[2])
			}
			na.Ends = ts
		}
	}

	return &na, nil
}

// alterEventIter is the row iterator for *CreateEvent.
type alterEventIter struct {
	once          sync.Once
	originalName  string
	alterSchedule bool
	alterStatus   bool
	eventDetails  sql.EventDetails
	eventDb       sql.EventDatabase
}

// Next implements the sql.RowIter interface.
func (c *alterEventIter) Next(ctx *sql.Context) (sql.Row, error) {
	run := false
	c.once.Do(func() {
		run = true
	})
	if !run {
		return nil, io.EOF
	}

	var eventEnded = false
	if c.eventDetails.HasExecuteAt {
		eventEnded = c.eventDetails.ExecuteAt.Sub(c.eventDetails.LastAltered).Seconds() < 0
	} else if c.eventDetails.HasEnds {
		eventEnded = c.eventDetails.Ends.Sub(c.eventDetails.LastAltered).Seconds() < 0
	}

	if eventEnded {
		// If the event execution/end time is altered and in the past.
		if c.alterSchedule {
			if c.eventDetails.OnCompletionPreserve {
				// If ON COMPLETION PRESERVE is defined, the event is disabled.
				c.eventDetails.Status = EventStatus_Disable.String()
				ctx.Session.Warn(&sql.Warning{
					Level:   "Note",
					Code:    1544,
					Message: "Event execution time is in the past. Event has been disabled",
				})
			} else {
				return nil, fmt.Errorf("Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was not changed. Specify a time in the future.")
			}
		}

		if c.alterStatus {
			if c.eventDetails.OnCompletionPreserve {
				// If the event execution/end time is in the past and is ON COMPLETION PRESERVE, status must stay as DISABLE.
				c.eventDetails.Status = EventStatus_Disable.String()
			} else {
				// If event status was set to ENABLE and ON COMPLETION NOT PRESERVE, it gets dropped.
				err := c.eventDb.DropEvent(ctx, c.originalName)
				if err != nil {
					return nil, err
				}
				return sql.Row{types.NewOkResult(0)}, nil
			}
		}
	}

	var eventDefinition = sql.EventDefinition{
		Name:            c.eventDetails.Name,
		CreateStatement: c.eventDetails.CreateEventStatement(),
		CreatedAt:       c.eventDetails.Created,
		LastAltered:     c.eventDetails.LastAltered,
	}

	err := c.eventDb.UpdateEvent(ctx, c.originalName, eventDefinition)
	if err != nil {
		return nil, err
	}

	// If Starts is set to current_timestamp or not set, then execute the event once and update last executed At.
	if c.eventDetails.LastAltered.Sub(c.eventDetails.Starts).Abs().Seconds() <= 1 {
		// TODO: execute the event once and update 'LastExecuted' and 'ExecutionCount'
	}

	return sql.Row{types.NewOkResult(0)}, nil
}

// Close implements the sql.RowIter interface.
func (c *alterEventIter) Close(ctx *sql.Context) error {
	return nil
}
