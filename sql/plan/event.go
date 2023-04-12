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
	"strconv"
	"strings"
)

// EventStatus represents an event status that is defined for an event.
type EventStatus byte

const (
	EventStatus_Enable EventStatus = iota
	EventStatus_Disable
	EventStatus_DisableOnSlave
)

// String returns the original SQL representation.
func (e EventStatus) String() string {
	switch e {
	case EventStatus_Enable:
		return "ENABLE"
	case EventStatus_Disable:
		return "DISABLE"
	case EventStatus_DisableOnSlave:
		return "DISABLE ON SLAVE"
	default:
		panic(fmt.Errorf("invalid event status value `%d`", byte(e)))
	}
}

// EventStatusFromString returns EventStatus based on the given string value.
// This function is used in Dolt to get EventStatus value for the EventDetails.
func EventStatusFromString(status string) (EventStatus, error) {
	switch strings.ToLower(status) {
	case "enable":
		return EventStatus_Enable, nil
	case "disable":
		return EventStatus_Disable, nil
	case "disable on slave":
		return EventStatus_DisableOnSlave, nil
	default:
		// use disable as default to be safe
		return EventStatus_Disable, fmt.Errorf("invalid event status value: `%s`", status)
	}
}

// EventOnScheduleEveryInterval is used to store ON SCHEDULE EVERY clause's interval definition.
// It is equivalent of expression.TimeDelta without microseconds field.
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

// GetIntervalValAndField returns ON SCHEDULE EVERY clause's
// interval value and field type in string format
// (e.g. returns "'1:2'" and "MONTH_DAY" for 1 month and 2 day
// or returns "4" and "HOUR" for 4 hour intervals).
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

// EventOnScheduleEveryIntervalFromString returns *EventOnScheduleEveryInterval parsing
// given interval string such as `2 DAY` or `'1:2' MONTH_DAY`. This function is used in Dolt to construct
// EventOnScheduleEveryInterval value for the EventDetails.
func EventOnScheduleEveryIntervalFromString(every string) (*EventOnScheduleEveryInterval, error) {
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
