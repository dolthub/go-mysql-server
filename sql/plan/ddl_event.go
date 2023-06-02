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
	gmstime "github.com/dolthub/go-mysql-server/internal/time"
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
var _ sql.EventSchedulerNotifierStatement = (*CreateEvent)(nil)

type CreateEvent struct {
	ddlNode
	EventName        string
	Definer          string
	At               *OnScheduleTimestamp
	Every            *expression.Interval
	Starts           *OnScheduleTimestamp
	Ends             *OnScheduleTimestamp
	OnCompPreserve   bool
	Status           sql.EventStatus
	Comment          string
	DefinitionString string
	DefinitionNode   sql.Node
	IfNotExists      bool
	// notifier is used to notify EventSchedulerStatus of the event creation
	notifier sql.EventSchedulerNotifier
}

// NewCreateEvent returns a *CreateEvent node.
func NewCreateEvent(
	db sql.Database,
	name, definer string,
	at, starts, ends *OnScheduleTimestamp,
	every *expression.Interval,
	onCompletionPreserve bool,
	status sql.EventStatus,
	comment, definitionString string,
	definition sql.Node,
	ifNotExists bool,
) *CreateEvent {
	return &CreateEvent{
		ddlNode:          ddlNode{db},
		EventName:        name,
		Definer:          definer,
		At:               at,
		Every:            every,
		Starts:           starts,
		Ends:             ends,
		OnCompPreserve:   onCompletionPreserve,
		Status:           status,
		Comment:          comment,
		DefinitionString: definitionString,
		DefinitionNode:   definition,
		IfNotExists:      ifNotExists,
	}
}

// Resolved implements the sql.Node interface.
func (c *CreateEvent) Resolved() bool {
	r := c.ddlNode.Resolved() && c.DefinitionNode.Resolved()
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
	return []sql.Node{c.DefinitionNode}
}

// WithChildren implements the sql.Node interface.
func (c *CreateEvent) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 1)
	}

	nc := *c
	nc.DefinitionNode = children[0]

	return &nc, nil
}

// CheckPrivileges implements the interface sql.Node.
func (c *CreateEvent) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(c.Db.Name(), "", "", sql.PrivilegeType_Event))
}

// Database implements the sql.Databaser interface.
func (c *CreateEvent) Database() sql.Database {
	return c.Db
}

// WithDatabase implements the sql.Databaser interface.
func (c *CreateEvent) WithDatabase(database sql.Database) (sql.Node, error) {
	ce := *c
	ce.Db = database
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
		onSchedule = fmt.Sprintf(" ON SCHEDULE %s", c.At.String())
	} else {
		onSchedule = onScheduleEveryString(c.Every, c.Starts, c.Ends)
	}

	onCompletion := ""
	if !c.OnCompPreserve {
		onCompletion = fmt.Sprintf(" ON COMPLETION NOT PRESERVE")
	}

	comment := ""
	if c.Comment != "" {
		comment = fmt.Sprintf(" COMMENT '%s'", c.Comment)
	}

	return fmt.Sprintf("CREATE%s EVENT %s %s%s%s%s DO %s",
		definer, c.EventName, onSchedule, onCompletion, c.Status.String(), comment, sql.DebugString(c.DefinitionNode))
}

// Expressions implements the sql.Expressioner interface.
func (c *CreateEvent) Expressions() []sql.Expression {
	if c.At != nil {
		return []sql.Expression{c.At}
	} else {
		if c.Starts == nil && c.Ends == nil {
			return []sql.Expression{c.Every}
		} else if c.Starts == nil {
			return []sql.Expression{c.Every, c.Ends}
		} else if c.Ends == nil {
			return []sql.Expression{c.Every, c.Starts}
		} else {
			return []sql.Expression{c.Every, c.Starts, c.Ends}
		}
	}
}

// WithExpressions implements the sql.Expressioner interface.
func (c *CreateEvent) WithExpressions(e ...sql.Expression) (sql.Node, error) {
	if len(e) < 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(e), "at least 1")
	}

	nc := *c
	if c.At != nil {
		ts, ok := e[0].(*OnScheduleTimestamp)
		if !ok {
			return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", e[0])
		}
		nc.At = ts
	} else {
		every, ok := e[0].(*expression.Interval)
		if !ok {
			return nil, fmt.Errorf("expected `*expression.Interval` but got `%T`", e[0])
		}
		nc.Every = every

		var ts *OnScheduleTimestamp
		if len(e) > 1 {
			ts, ok = e[1].(*OnScheduleTimestamp)
			if !ok {
				return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", e[1])
			}
			if c.Starts != nil {
				nc.Starts = ts
			} else if c.Ends != nil {
				nc.Ends = ts
			}
		}

		if len(e) == 3 {
			ts, ok = e[2].(*OnScheduleTimestamp)
			if !ok {
				return nil, fmt.Errorf("expected `*OnScheduleTimestamp` but got `%T`", e[2])
			}
			nc.Ends = ts
		}
	}

	return &nc, nil
}

// RowIter implements the sql.Node interface.
func (c *CreateEvent) RowIter(ctx *sql.Context, row sql.Row) (sql.RowIter, error) {
	eventCreationTime := ctx.QueryTime()
	// TODO: event time values are evaluated in 'SYSTEM' TZ for now (should be session TZ)
	eventDetails, err := c.GetEventDetails(ctx, eventCreationTime, eventCreationTime, time.Time{}, gmstime.SystemTimezoneOffset())
	if err != nil {
		return nil, err
	}

	eventDb, ok := c.Db.(sql.EventDatabase)
	if !ok {
		return nil, sql.ErrEventsNotSupported.New(c.Db.Name())
	}

	return &createEventIter{
		eventDetails: eventDetails,
		eventDb:      eventDb,
		ifNotExists:  c.IfNotExists,
		notifier:     c.notifier,
	}, nil
}

// WithEventSchedulerNotifier is used to notify EventSchedulerStatus to update the events list for CREATE EVENT.
func (c *CreateEvent) WithEventSchedulerNotifier(notifier sql.EventSchedulerNotifier) sql.Node {
	nc := *c
	nc.notifier = notifier
	return &nc
}

// GetEventDetails returns eventDetails based on CreateEvent object.
// It expects all timestamp and interval values to be resolved.
// This function gets called either from RowIter of CreateEvent plan,
// or from analyzer getting EventDetails from EventDefinition retrieved from a database.
func (c *CreateEvent) GetEventDetails(ctx *sql.Context, eventCreationTime, lastAltered, lastExecuted time.Time, tz string) (sql.EventDetails, error) {
	eventDetails := sql.EventDetails{
		Name:                 c.EventName,
		Definer:              c.Definer,
		OnCompletionPreserve: c.OnCompPreserve,
		Status:               c.Status.String(),
		Comment:              c.Comment,
		Definition:           c.DefinitionString,
		TimezoneOffset:       tz,
	}

	if c.At != nil {
		var err error
		eventDetails.HasExecuteAt = true
		eventDetails.ExecuteAt, err = c.At.EvalTime(ctx, tz)
		if err != nil {
			return sql.EventDetails{}, err
		}
	} else {
		delta, err := c.Every.EvalDelta(ctx, nil)
		if err != nil {
			return sql.EventDetails{}, err
		}
		interval := sql.NewEveryInterval(delta.Years, delta.Months, delta.Days, delta.Hours, delta.Minutes, delta.Seconds)
		iVal, iField := interval.GetIntervalValAndField()
		eventDetails.ExecuteEvery = fmt.Sprintf("%s %s", iVal, iField)

		if c.Starts != nil {
			eventDetails.Starts, err = c.Starts.EvalTime(ctx, tz)
			if err != nil {
				return sql.EventDetails{}, err
			}
		} else {
			// If STARTS is not defined, it defaults to CURRENT_TIMESTAMP
			eventDetails.Starts = eventCreationTime
		}
		if c.Ends != nil {
			eventDetails.HasEnds = true
			eventDetails.Ends, err = c.Ends.EvalTime(ctx, tz)
			if err != nil {
				return sql.EventDetails{}, err
			}
		}
	}

	eventDetails.Created = eventCreationTime
	eventDetails.LastAltered = lastAltered
	eventDetails.LastExecuted = lastExecuted
	return eventDetails, nil
}

// createEventIter is the row iterator for *CreateEvent.
type createEventIter struct {
	once         sync.Once
	eventDetails sql.EventDetails
	eventDb      sql.EventDatabase
	ifNotExists  bool
	notifier     sql.EventSchedulerNotifier
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

	// checks if the defined ENDS time is before STARTS time
	if c.eventDetails.HasEnds {
		if c.eventDetails.Ends.Sub(c.eventDetails.Starts).Seconds() < 0 {
			return nil, fmt.Errorf("ENDS is either invalid or before STARTS")
		}
	}

	err := c.eventDb.SaveEvent(ctx, c.eventDetails)
	if err != nil {
		if sql.ErrEventAlreadyExists.Is(err) && c.ifNotExists {
			ctx.Session.Warn(&sql.Warning{
				Level:   "Note",
				Code:    1537,
				Message: fmt.Sprintf(err.Error()),
			})
			return sql.Row{types.NewOkResult(0)}, nil
		}
		return nil, err
	}

	if c.eventDetails.HasExecuteAt {
		// If the event execution time is in the past and is set.
		// TODO: executeAt value should be converted from event TZ to system TZ
		//  for checking event should be executed or dropped.
		//  (created time value should be current time in system TZ)
		if c.eventDetails.ExecuteAt.Sub(c.eventDetails.Created).Seconds() < 0 {
			if c.eventDetails.OnCompletionPreserve {
				// If ON COMPLETION PRESERVE is defined, the event is disabled.
				c.eventDetails.Status = sql.EventStatus_Disable.String()
				err = c.eventDb.UpdateEvent(ctx, c.eventDetails.Name, c.eventDetails)
				if err != nil {
					return nil, err
				}
				ctx.Session.Warn(&sql.Warning{
					Level:   "Note",
					Code:    1544,
					Message: fmt.Sprintf("Event execution time is in the past. Event has been disabled"),
				})
			} else {
				// If ON COMPLETION NOT PRESERVE is defined, the event is dropped immediately after creation.
				err = c.eventDb.DropEvent(ctx, c.eventDetails.Name)
				if err != nil {
					return nil, err
				}
				ctx.Session.Warn(&sql.Warning{
					Level:   "Note",
					Code:    1588,
					Message: fmt.Sprintf("Event execution time is in the past and ON COMPLETION NOT PRESERVE is set. The event was dropped immediately after creation."),
				})
			}
			return sql.Row{types.NewOkResult(0)}, nil
		}
	}

	// make sure to notify the EventSchedulerStatus AFTER adding the event in the database
	if c.notifier != nil {
		c.notifier.AddEvent(ctx, c.eventDb, c.eventDetails)
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
		startsStr = fmt.Sprintf(" %s", starts.String())
	}
	endsStr := ""
	if ends != nil {
		endsStr = fmt.Sprintf(" %s", ends.String())
	}

	return fmt.Sprintf("ON SCHEDULE EVERY %s%s%s", everyInterval, startsStr, endsStr)
}

// OnScheduleTimestamp is object used for EVENT ON SCHEDULE { AT / STARTS / ENDS } optional fields only.
type OnScheduleTimestamp struct {
	field     string
	timestamp sql.Expression
	intervals []sql.Expression
}

var _ sql.Expression = (*OnScheduleTimestamp)(nil)

// NewOnScheduleTimestamp creates OnScheduleTimestamp object used for EVENT ON SCHEDULE { AT / STARTS / ENDS } optional fields only.
func NewOnScheduleTimestamp(f string, ts sql.Expression, i []sql.Expression) *OnScheduleTimestamp {
	return &OnScheduleTimestamp{
		field:     f,
		timestamp: ts,
		intervals: i,
	}
}

func (ost *OnScheduleTimestamp) Type() sql.Type {
	return ost.timestamp.Type()
}

func (ost *OnScheduleTimestamp) IsNullable() bool {
	if ost.timestamp.IsNullable() {
		return true
	}
	for _, i := range ost.intervals {
		if i.IsNullable() {
			return true
		}
	}
	return false
}

func (ost *OnScheduleTimestamp) Children() []sql.Expression {
	var exprs = []sql.Expression{ost.timestamp}
	return append(exprs, ost.intervals...)
}

func (ost *OnScheduleTimestamp) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) == 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(ost, len(children), "at least 1")
	}

	var intervals = make([]sql.Expression, 0)
	if len(children) > 1 {
		intervals = append(intervals, children[1:]...)
	}

	return NewOnScheduleTimestamp(ost.field, children[0], intervals), nil
}

// Resolved implements the sql.Node interface.
func (ost *OnScheduleTimestamp) Resolved() bool {
	var children = []sql.Expression{ost.timestamp}
	children = append(children, ost.intervals...)
	for _, child := range children {
		if !child.Resolved() {
			return false
		}
	}
	return true
}

// String implements the sql.Node interface.
func (ost *OnScheduleTimestamp) String() string {
	intervals := ""
	for _, interval := range ost.intervals {
		intervals = fmt.Sprintf("%s + %s", intervals, interval.String())
	}
	return fmt.Sprintf("%s %s%s", ost.field, ost.timestamp.String(), intervals)
}

func (ost *OnScheduleTimestamp) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("OnScheduleTimestamp.Eval is just a placeholder method and should not be called directly")
}

// EvalTime returns time.Time value converted to UTC evaluating given expressions
// as expected to be time value and optional interval values. The value returned
// is time.Time value from timestamp value plus all intervals given.
func (ost *OnScheduleTimestamp) EvalTime(ctx *sql.Context, tz string) (time.Time, error) {
	value, err := ost.timestamp.Eval(ctx, nil)
	if err != nil {
		return time.Time{}, err
	}

	if bs, ok := value.([]byte); ok {
		value = string(bs)
	}

	var t time.Time
	switch v := value.(type) {
	case time.Time:
		// TODO: check if this value is in session timezone
		t = v
	case string:
		t, err = sql.GetTimeValueFromStringInput(ost.field, v)
		if err != nil {
			return time.Time{}, err
		}
	default:
		return time.Time{}, fmt.Errorf("unexpected type: %s", v)
	}

	for _, interval := range ost.intervals {
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

	// truncates the timezone part from the time value and returns the time value in given TZ
	truncatedVal, err := time.Parse(sql.EventDateSpaceTimeFormat, t.Format(sql.EventDateSpaceTimeFormat))
	if err != nil {
		return time.Time{}, err
	}
	return gmstime.ConvertTimeToLocation(truncatedVal, tz)
}

var _ sql.Node = (*DropEvent)(nil)
var _ sql.Databaser = (*DropEvent)(nil)
var _ sql.EventSchedulerNotifierStatement = (*DropEvent)(nil)

type DropEvent struct {
	ddlNode
	EventName string
	IfExists  bool
	// notifier is used to notify EventSchedulerStatus of the event deletion
	notifier sql.EventSchedulerNotifier
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
	eventDb, ok := d.Db.(sql.EventDatabase)
	if !ok {
		if d.IfExists {
			return sql.RowsToRowIter(), nil
		} else {
			return nil, sql.ErrEventsNotSupported.New(d.EventName)
		}
	}

	err := eventDb.DropEvent(ctx, d.EventName)
	if d.IfExists && sql.ErrEventDoesNotExist.Is(err) {
		ctx.Session.Warn(&sql.Warning{
			Level:   "Note",
			Code:    1305,
			Message: fmt.Sprintf("Event %s does not exist", d.EventName),
		})
	} else if err != nil {
		return nil, err
	}

	// make sure to notify the EventSchedulerStatus after dropping the event in the database
	if d.notifier != nil {
		d.notifier.RemoveEvent(eventDb.Name(), d.EventName)
	}

	return sql.RowsToRowIter(sql.Row{types.NewOkResult(0)}), nil
}

// WithChildren implements the sql.Node interface.
func (d *DropEvent) WithChildren(children ...sql.Node) (sql.Node, error) {
	return NillaryWithChildren(d, children...)
}

// CheckPrivileges implements the interface sql.Node.
func (d *DropEvent) CheckPrivileges(ctx *sql.Context, opChecker sql.PrivilegedOperationChecker) bool {
	return opChecker.UserHasPrivileges(ctx,
		sql.NewPrivilegedOperation(d.Db.Name(), "", "", sql.PrivilegeType_Event))
}

// WithDatabase implements the sql.Databaser interface.
func (d *DropEvent) WithDatabase(database sql.Database) (sql.Node, error) {
	nde := *d
	nde.Db = database
	return &nde, nil
}

// WithEventSchedulerNotifier is used to notify EventSchedulerStatus to update the events list for DROP EVENT.
func (d *DropEvent) WithEventSchedulerNotifier(notifier sql.EventSchedulerNotifier) sql.Node {
	nd := *d
	nd.notifier = notifier
	return &nd
}
