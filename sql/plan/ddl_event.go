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
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

var _ sql.Node = (*CreateEvent)(nil)
var _ sql.Expressioner = (*CreateEvent)(nil)
var _ sql.Databaser = (*CreateEvent)(nil)
var _ sql.DebugStringer = (*CreateEvent)(nil)

type CreateEvent struct {
	*Event
	ddlNode
	At         *OnScheduleTimestamp
	Every      sql.Expression
	Starts     *OnScheduleTimestamp
	Ends       *OnScheduleTimestamp
	BodyString string
}

func (c *CreateEvent) Expressions() []sql.Expression {
	if c.Every != nil {
		return []sql.Expression{c.Every}
	}
	return nil
}

func (c *CreateEvent) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	if len(e) > 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(e), "0 or 1")
	}

	if len(e) == 0 {
		return c, nil
	}

	every, ok := e[0].(*expression.Interval)
	if ok {
		return nil, fmt.Errorf("expected `*expression.Interval` but got `%T`", e[0])
	}

	nc := *c
	nc.Every = every
	return &nc, nil
}

// NewCreateEvent returns a *CreateEvent node.
func NewCreateEvent(
	db sql.Database,
	name, definer string,
	onCompletionPreserve bool,
	status sql.EventStatus,
	definition sql.Node,
	comment, query, bodyString string,
	at, starts, ends *OnScheduleTimestamp,
	every sql.Expression,
) *CreateEvent {
	e := NewEvent(db.Name(), name, definer, onCompletionPreserve, status, definition, comment, query)
	return &CreateEvent{
		Event:      e,
		ddlNode:    ddlNode{db},
		BodyString: bodyString,
		At:         at,
		Every:      every,
		Starts:     starts,
		Ends:       ends,
	}
}

// Resolved implements the sql.Node interface.
func (c *CreateEvent) Resolved() bool {
	r := c.ddlNode.Resolved() && c.Event.Resolved()
	if c.At != nil {
		r = r && c.At.Resolved()
	} else {
		r = r && c.Every.Resolved()
		if c.Starts != nil {
			r = r && c.Starts.Resolved()
		}
		if c.Ends != nil {
			r = r && c.Ends.Resolved()
		}
	}
	return r
}

// Schema implements the sql.Node interface.
func (c *CreateEvent) Schema() sql.Schema {
	return nil
}

// Children implements the sql.Node interface.
func (c *CreateEvent) Children() []sql.Node {
	if c.At != nil {
		return []sql.Node{c.Event, c.At}
	} else {
		if c.Starts == nil && c.Ends == nil {
			return []sql.Node{c.Event}
		} else if c.Starts == nil {
			return []sql.Node{c.Event, c.Ends}
		} else if c.Ends == nil {
			return []sql.Node{c.Event, c.Starts}
		} else {
			return []sql.Node{c.Event, c.Starts, c.Ends}
		}
	}
}

// WithChildren implements the sql.Node interface.
func (c *CreateEvent) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) == 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), "at least 1")
	}
	event, ok := children[0].(*Event)
	if !ok {
		return nil, fmt.Errorf("expected `*Event` but got `%T`", children[0])
	}

	nc := *c
	nc.Event = event

	if len(children) > 1 {
		ts, ok := children[1].(*OnScheduleTimestamp)
		if !ok {
			return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", children[0])
		}
		if c.At != nil {
			nc.At = ts
		} else {
			if c.Starts != nil {
				nc.Starts = ts
			} else if c.Ends != nil {
				nc.Ends = ts
			}
			if len(children) == 3 {
				ts2, ok := children[2].(*OnScheduleTimestamp)
				if !ok {
					return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", children[0])
				}
				nc.Ends = ts2
			}
		}
	}

	return &nc, nil
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateEvent) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(c.db.Name(), "", "", sql.PrivilegeType_Event))
}

// Database implements the sql.Databaser interface.
func (c *CreateEvent) Database() sql.Database {
	return c.db
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateEvent) WithDatabase(database sql.Database) (sql.Node, error) {
	ce := *c
	ce.db = database
	return &ce, nil
}

// String implements the sql.Node interface.
func (c *CreateEvent) String() string {
	definer := ""
	if c.Definer != "" {
		definer = fmt.Sprintf(" DEFINER = %s", c.Definer)
	}

	onSchedule := ""
	if c.At != nil {
		onSchedule = fmt.Sprintf(" ON SCHEDULE AT %s", c.At.String())
	} else {
		onSchedule = onScheduleEveryString(c.Every, c.Starts, c.Ends)
	}

	onCompletion := ""
	if !c.OnCompletionPreserve {
		onCompletion = fmt.Sprintf(" ON COMPLETION NOT PRESERVE")
	}

	comment := ""
	if c.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", c.Comment)
	}

	return fmt.Sprintf("CREATE%s EVENT %s %s%s%s%s DO %s",
		definer, c.Name, onSchedule, onCompletion, c.Status.String(), comment, sql.DebugString(c.Event))
}

// DebugString implements the sql.DebugStringer interface.
func (c *CreateEvent) DebugString() string {
	return c.String()
}

// RowIter implements the sql.Node interface.
func (c *CreateEvent) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	eventDetails := sql.EventDetails{
		SchemaName:           c.db.Name(),
		Name:                 c.Name,
		Definer:              c.Definer,
		Definition:           c.BodyString,
		Status:               c.Status,
		OnCompletionPreserve: c.OnCompletionPreserve,
		Created:              c.Created,
		LastAltered:          c.LastAltered,
		LastExecuted:         c.LastExecuted,
		ExecutionCount:       c.ExecutionCount,
		Comment:              c.Comment,
		CreateStatement:      c.CreateStatement,
	}

	eventCreationTime := time.Now()
	var err error
	if c.At != nil {
		eventDetails.HasExecuteAt = true
		eventDetails.ExecuteAt, err = c.At.EvalTime(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		i, ok := c.Every.(*expression.Interval)
		if !ok {
			return nil, fmt.Errorf("expected interval but got: %s", c.Every)
		}
		delta, err := i.EvalDelta(ctx, nil)
		if err != nil {
			return nil, err
		}

		eventDetails.ExecuteEvery = sql.NewEveryInterval(delta.Years, delta.Months, delta.Days, delta.Hours, delta.Minutes, delta.Seconds)

		if c.Starts != nil {
			eventDetails.HasStarts = true
			eventDetails.Starts, err = c.Starts.EvalTime(ctx)
			if err != nil {
				return nil, err
			}
		} else {
			// If STARTS is not defined, it defaults to CURRENT_TIMESTAMP
			eventDetails.Starts = eventCreationTime
		}
		if c.Ends != nil {
			eventDetails.HasEnds = true
			eventDetails.Ends, err = c.Ends.EvalTime(ctx)
			if err != nil {
				return nil, err
			}
		}
	}

	eventDetails.Created = eventCreationTime
	eventDetails.LastAltered = eventCreationTime

	return &createEventIter{
		ed: eventDetails,
		db: c.db,
	}, nil
}

// createEventIter is the row iterator for *CreateEvent.
type createEventIter struct {
	once sync.Once
	ed   sql.EventDetails
	db   sql.Database
}

// Next implements the sql.RowIter interface.
func (c *createEventIter) Next(ctx *sql.Context) (sql.Row, error) {
	run := false
	c.once.Do(func() {
		run = true
	})
	if !run {
		return nil, io.EOF
	}

	pdb, ok := c.db.(sql.EventDatabase)
	if !ok {
		return nil, sql.ErrEventsNotSupported.New(c.db.Name())
	}

	if c.ed.HasEnds {
		if c.ed.Ends.Sub(c.ed.Starts).Seconds() < 0 {
			return nil, fmt.Errorf("ENDS is either invalid or before STARTS")
		}
	}

	err := pdb.SaveEvent(ctx, c.ed)
	if err != nil {
		return nil, err
	}

	// If the event execution time is in the past and ON COMPLETION NOT PRESERVE is set.
	// The event is dropped immediately after creation.
	if c.ed.HasExecuteAt {
		if c.ed.ExecuteAt.Sub(c.ed.Created).Seconds() < 0 && !c.ed.OnCompletionPreserve {
			err = pdb.DropEvent(ctx, c.ed.Name)
			if err != nil {
				return nil, err
			}

			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    1588,
				Message: fmt.Sprintf("Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation."),
			})
			return sql.Row{types.NewOkResult(0)}, nil
		}
	}

	// If Starts is set to current_timestamp or not set, then execute the event once and update last executed At.
	if c.ed.Created.Sub(c.ed.Starts).Abs().Seconds() <= 1 {
		// TODO: execute the event once
	}

	return sql.Row{types.NewOkResult(0)}, nil
}

// Close implements the sql.RowIter interface.
func (c *createEventIter) Close(ctx *sql.Context) error {
	return nil
}

// onScheduleEveryString returns ON SCHEDULE EVERY clause part of CREATE EVENT statement.
func onScheduleEveryString(every sql.Expression, starts, ends *OnScheduleTimestamp) string {
	everyInterval := strings.TrimPrefix(every.String(), "INTERVAL ")

	startsStr := ""
	if starts != nil {
		startsStr = fmt.Sprintf(" STARTS %s", starts.String())
	}

	endsStr := ""
	if ends != nil {
		endsStr = fmt.Sprintf(" ENDS %s", ends.String())
	}

	return fmt.Sprintf("ON SCHEDULE EVERY %s%s%s", everyInterval, startsStr, endsStr)
}

// OnScheduleTimestamp is object used for EVENT ON SCHEDULE { AT / STARTS / ENDS } optional fields only.
type OnScheduleTimestamp struct {
	timestamp sql.Expression
	intervals []sql.Expression
}

var _ sql.Node = (*OnScheduleTimestamp)(nil)
var _ sql.Expressioner = (*OnScheduleTimestamp)(nil)

func NewEventOnScheduleTimestamp(ts sql.Expression, i []sql.Expression) *OnScheduleTimestamp {
	return &OnScheduleTimestamp{
		timestamp: ts,
		intervals: i,
	}
}

func (a *OnScheduleTimestamp) Resolved() bool {
	var children = []sql.Expression{a.timestamp}
	children = append(children, a.intervals...)
	for _, child := range children {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

func (a *OnScheduleTimestamp) String() string {
	intervals := ""
	for _, interval := range a.intervals {
		intervals = fmt.Sprintf("%s + %s", intervals, interval.String())
	}
	return fmt.Sprintf("%s%s", a.timestamp.String(), intervals)
}

func (a *OnScheduleTimestamp) Schema() sql.Schema {
	return nil
}

func (a *OnScheduleTimestamp) Children() []sql.Node {
	return nil
}

func (a *OnScheduleTimestamp) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	panic("OnScheduleTimestamp.RowIter is just a placeholder method and should not be called directly")
}

func (a *OnScheduleTimestamp) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(a, children...)
}

func (a *OnScheduleTimestamp) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return true
}

func (a *OnScheduleTimestamp) Expressions() []sql.Expression {
	var exprs = []sql.Expression{a.timestamp}
	return append(exprs, a.intervals...)
}

func (a *OnScheduleTimestamp) WithExpressions(exprs ...sql.Expression) (sql.Node, error) {
	if len(exprs) == 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(a, len(exprs), "at least 1")
	}

	var intervals = make([]sql.Expression, 0)
	if len(exprs) > 1 {
		intervals = append(intervals, exprs[1:]...)
	}

	return NewEventOnScheduleTimestamp(exprs[0], intervals), nil
}

// EvalTime evaluates time.Time value for given expressions as expected to be time value and optional
// interval values. The value returned is time.Time value from timestamp value plus all intervals given.
func (a *OnScheduleTimestamp) EvalTime(ctx *sql.Context) (time.Time, error) {
	value, err := a.timestamp.Eval(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}
	var t time.Time
	switch v := value.(type) {
	case time.Time:
		t = v
	case string, []byte:
		d, err := types.Datetime.Convert(v)
		if err != nil {
			return time.Time{}, err
		}
		tt, ok := d.(time.Time)
		if !ok {
			return time.Time{}, fmt.Errorf("expected time.Time type but got: %s", d)
		}
		t = tt
	default:
		return time.Time{}, fmt.Errorf("unexpected type: %s", v)
	}

	for _, interval := range a.intervals {
		i, ok := interval.(*expression.Interval)
		if !ok {
			return time.Time{}, fmt.Errorf("expected interval but got: %s", interval)
		}

		timeDelta, err := i.EvalDelta(ctx, nil)
		if err != nil {
			return time.Time{}, err
		}
		t = timeDelta.Add(t)
	}

	return t, nil
}

var _ sql.Node = (*DropEvent)(nil)
var _ sql.Databaser = (*DropEvent)(nil)

type DropEvent struct {
	ddlNode
	EventName string
	IfExists  bool
}

// NewDropEvent creates a new *DropEvent node.
func NewDropEvent(db sql.Database, eventName string, ifExists bool) *DropEvent {
	return &DropEvent{
		ddlNode:   ddlNode{db},
		EventName: strings.ToLower(eventName),
		IfExists:  ifExists,
	}
}

// String implements the sql.Node interface.
func (d *DropEvent) String() string {
	ifExists := ""
	if d.IfExists {
		ifExists = "IF EXISTS "
	}
	return fmt.Sprintf("DROP PROCEDURE %s%s", ifExists, d.EventName)
}

// Schema implements the sql.Node interface.
func (d *DropEvent) Schema() sql.Schema {
	return nil
}

// RowIter implements the sql.Node interface.
func (d *DropEvent) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	eventDb, ok := d.db.(sql.EventDatabase)
	if !ok {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		} else {
			return nil, sql.ErrEventsNotSupported.New(d.EventName)
		}
	}
	err := eventDb.DropEvent(ctx, d.EventName)
	if d.IfExists && sql.ErrEventDoesNotExist.Is(err) {
		return sql.RowsToRowIter(), nil
	} else if err != nil {
		return nil, err
	}
	return sql.RowsToRowIter(), nil
}

// WithChildren implements the sql.Node interface.
func (d *DropEvent) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropEvent) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(d.db.Name(), "", "", sql.PrivilegeType_Event))
}

// WithDatabase implements the sql.Databaser interface.
func (d *DropEvent) WithDatabase(database sql.Database) (sql.Node, error) {
	nde := *d
	nde.db = database
	return &nde, nil
}
