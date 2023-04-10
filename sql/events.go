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

package sql

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const EventTimeStampFormat = "2006-01-02 15:04:05"

// EventDetails are the details of the event.
type EventDetails struct {
	Name                 string
	Definer              string
	OnCompletionPreserve bool
	Status               EventStatus
	Comment              string
	Definition           string

	// events' ON SCHEDULE clause fields
	ExecuteAt    time.Time
	HasExecuteAt bool
	ExecuteEvery *EventOnScheduleEveryInterval
	Starts       time.Time
	HasStarts    bool
	Ends         time.Time
	HasEnds      bool

	// time values of event create/alter/execute metadata
	Created        time.Time
	LastAltered    time.Time
	LastExecuted   time.Time
	ExecutionCount uint64
	// TODO: add TimeZone
}

func (e *EventDetails) GetCreateEventStatement() string {
	stmt := fmt.Sprintf("CREATE")
	if e.Definer != "" {
		stmt = fmt.Sprintf("%s DEFINER = %s", stmt, e.Definer)
	}
	stmt = fmt.Sprintf("%s EVENT `%s`", stmt, e.Name)

	if e.HasExecuteAt {
		stmt = fmt.Sprintf("%s ON SCHEDULE AT '%s'", stmt, e.ExecuteAt.Format(EventTimeStampFormat))
	} else {
		val, field := e.ExecuteEvery.GetIntervalValAndField()
		// STARTS should be NOT null regardless of user definition
		stmt = fmt.Sprintf("%s ON SCHEDULE EVERY %s %s STARTS '%s'", stmt, val, field, e.Starts.Format(EventTimeStampFormat))
		if e.HasEnds {
			stmt = fmt.Sprintf("%s ENDS '%s'", stmt, e.Ends.Format(EventTimeStampFormat))
		}
	}

	if e.OnCompletionPreserve {
		stmt = fmt.Sprintf("%s ON COMPLETION PRESERVE", stmt)
	} else {
		stmt = fmt.Sprintf("%s ON COMPLETION NOT PRESERVE", stmt)
	}

	stmt = fmt.Sprintf("%s %s", stmt, e.Status.String())

	if e.Comment != "" {
		stmt = fmt.Sprintf("%s COMMENT '%s'", stmt, e.Comment)
	}

	stmt = fmt.Sprintf("%s DO %s", stmt, e.Definition)
	return stmt
}

// EventStatus represents an event status that is defined for an event.
type EventStatus byte

const (
	EventStatus_Enable EventStatus = iota
	EventStatus_Disable
	EventStatus_DisableOnSlove
)

// String returns the original SQL representation.
func (e EventStatus) String() string {
	switch e {
	case EventStatus_Enable:
		return "ENABLE"
	case EventStatus_Disable:
		return "DISABLE"
	case EventStatus_DisableOnSlove:
		return "DISABLE ON SLAVE"
	default:
		panic(fmt.Errorf("invalid event status value `%d`", byte(e)))
	}
}

func GetEventStatusFromString(status string) (EventStatus, error) {
	switch strings.ToLower(status) {
	case "enable":
		return EventStatus_Enable, nil
	case "disable":
		return EventStatus_Disable, nil
	case "disable on slave":
		return EventStatus_DisableOnSlove, nil
	default:
		// use disable as default to be safe
		return EventStatus_Disable, fmt.Errorf("invalid event status value: `%s`", status)
	}
}

type EventOnScheduleEveryInterval struct {
	Years   int64
	Months  int64
	Days    int64
	Hours   int64
	Minutes int64
	Seconds int64
}

func NewEveryInterval(y, mo, d, h, mi, s int64) *EventOnScheduleEveryInterval {
	return &EventOnScheduleEveryInterval{
		Years:   y,
		Months:  mo,
		Days:    d,
		Hours:   h,
		Minutes: mi,
		Seconds: s,
	}
}

// GetIntervalValAndField
func (e *EventOnScheduleEveryInterval) GetIntervalValAndField() (string, string) {
	if e == nil {
		return "", ""
	}

	var val, field []string
	if e.Years != 0 {
		val = append(val, fmt.Sprintf("%v", e.Years))
		field = append(field, "YEAR")
	}
	if e.Months != 0 {
		val = append(val, fmt.Sprintf("%v", e.Months))
		field = append(field, "MONTH")
	}
	if e.Days != 0 {
		val = append(val, fmt.Sprintf("%v", e.Days))
		field = append(field, "DAY")
	}
	if e.Hours != 0 {
		val = append(val, fmt.Sprintf("%v", e.Hours))
		field = append(field, "HOUR")
	}
	if e.Minutes != 0 {
		val = append(val, fmt.Sprintf("%v", e.Minutes))
		field = append(field, "MINUTE")
	}
	if e.Seconds != 0 {
		val = append(val, fmt.Sprintf("%v", e.Seconds))
		field = append(field, "SECOND")
	}

	if len(val) == 0 {
		return "", ""
	} else if len(val) == 1 {
		return val[0], field[0]
	}

	return fmt.Sprintf("'%s'", strings.Join(val, ":")), strings.Join(field, "_")
}

// GetEventOnScheduleEveryIntervalFromString returns *EventOnScheduleEveryInterval parsing
// given interval string such as `2 DAY` or `'1:2' MONTH_DAY`.
func GetEventOnScheduleEveryIntervalFromString(every string) (*EventOnScheduleEveryInterval, error) {
	errCannotParseEveryInterval := fmt.Errorf("cannot parse ON SCHEDULE EVERY interval: `%s`", every)
	strs := strings.Split(every, " ")
	if len(strs) != 2 {
		return nil, errCannotParseEveryInterval
	}
	intervalVal := strs[0]
	intervalField := strs[1]

	intervalVal = strings.TrimSuffix(strings.TrimPrefix(intervalVal, "'"), "'")
	iVals := strings.Split(intervalVal, ":")
	iFields := strings.Split(intervalField, "_")

	if len(iVals) != len(iFields) {
		return nil, errCannotParseEveryInterval
	}

	var interval = &EventOnScheduleEveryInterval{}
	for i, val := range iVals {
		n, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return nil, errCannotParseEveryInterval
		}
		switch iFields[i] {
		case "YEAR":
			interval.Years = n
		case "MONTH":
			interval.Months = n
		case "DAY":
			interval.Days = n
		case "HOUR":
			interval.Hours = n
		case "MINUTE":
			interval.Minutes = n
		case "SECOND":
			interval.Seconds = n
		default:
			return nil, errCannotParseEveryInterval
		}
	}

	return interval, nil
}
