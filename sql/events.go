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
	"strings"
	"time"
)



// EventDetails are the details of the event.
// TODO : should it be similar to procedures this case:
//  Integrators only need to store and retrieve the given
//  details for an event, as the engine handles all parsing and processing.
type EventDetails struct {
	SchemaName 				string
	Name  					string
	Definer    				string
	Definition 				string
	ExecuteAt  				time.Time
	HasExecuteAt	 		bool
	ExecuteEvery			*EventOnScheduleEveryInterval
	Starts        			time.Time
	HasStarts    			bool
	Ends          			time.Time
	HasEnds      			bool
	Status        			EventStatus
	OnCompletionPreserve 	bool
	Created 				time.Time
	LastAltered 			time.Time
	LastExecuted  			time.Time
	ExecutionCount 			uint64
	Comment    				string
	CreateStatement 		string

	// TODO: add TimeZone
}

// EventStatus represents an event status that is defined on an event.
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

type EventOnScheduleEveryInterval struct {
	Years        int64
	Months       int64
	Days         int64
	Hours        int64
	Minutes      int64
	Seconds      int64
}

func NewEveryInterval(y, mo, d, h, mi, s int64) *EventOnScheduleEveryInterval {
	return &EventOnScheduleEveryInterval{
		Years: y,
		Months: mo,
		Days: d,
		Hours: h,
		Minutes: mi,
		Seconds: s,
	}
}

func  (e *EventOnScheduleEveryInterval) GetIntervalValAndField() (string, string) {
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
