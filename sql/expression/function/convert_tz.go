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

package function

import (
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
)

type ConvertTz struct {
	dt     sql.Expression
	fromTz sql.Expression
	toTz   sql.Expression
}

var offsetRegex = regexp.MustCompile(`(?m)^\+(\d{2}):(\d{2})$`)

var _ sql.FunctionExpression = (*ConvertTz)(nil)

func NewConvertTz(ctx *sql.Context, dt, fromTz, toTz sql.Expression) sql.Expression {
	return &ConvertTz{
		dt:     dt,
		fromTz: fromTz,
		toTz:   toTz,
	}
}

func (c *ConvertTz) Resolved() bool {
	return c.dt.Resolved() && c.fromTz.Resolved() && c.toTz.Resolved()
}

func (c *ConvertTz) String() string {
	return fmt.Sprintf("CONVERT_TZ(%s, %s, %s)", c.dt, c.fromTz, c.toTz)
}

func (c *ConvertTz) Type() sql.Type {
	return sql.Datetime
}

func (c *ConvertTz) IsNullable() bool {
	return true
}

func (c *ConvertTz) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	from, err := c.fromTz.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	to, err := c.toTz.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	timestamp, err := c.dt.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	timestampStr, ok := timestamp.(string)
	if !ok {
		return nil, nil
	}

	fromStr, ok := from.(string)
	if !ok {
		return nil, nil
	}

	toStr, ok := to.(string)
	if !ok {
		return nil, nil
	}

	// Parse the timestamp into a time object.
	// Note: We could use sql.ConvertWithoutRangeCheck but we need the format of the outputted string.
	var dt time.Time
	var dFmt string
	for _, testFmt := range sql.TimestampDatetimeLayouts {
		if t, err := time.Parse(testFmt, timestampStr); err == nil {
			dFmt = testFmt
			dt = t
			break
		}
	}

	// We should return nil when we cannot parse the converted time
	if dFmt == "" {
		return nil, nil
	}

	timeZoneRes := convertTimeZone(dt, fromStr, toStr)
	if !timeZoneRes.IsZero() {
		return timeZoneRes.Format(dFmt), nil
	}

	// If we weren't successful converting by timezone try converting via durations
	timeZoneRes = convertDurations(dt, fromStr, toStr)
	if timeZoneRes.IsZero() {
		return nil, nil
	}

	return timeZoneRes.Format(dFmt), nil
}

func convertTimeZone(t time.Time, fromLocation string, toLocation string) time.Time {
	fLoc, err := time.LoadLocation(fromLocation)
	if err != nil {
		return time.Time{}
	}

	tLoc, err := time.LoadLocation(toLocation)
	if err != nil {
		return time.Time{}
	}

	fromTime := t.In(fLoc)

	return fromTime.In(tLoc)
}

func convertDurations(t time.Time, startDuration string, endDuration string) time.Time {
	fromDuration, err := getDeltaAsDuration(startDuration)
	if err != nil {
		return time.Time{}
	}

	toDuration, err := getDeltaAsDuration(endDuration)
	if err != nil {
		return time.Time{}
	}

	finalDuration := toDuration - fromDuration

	return t.Add(finalDuration)
}

func getDeltaAsDuration(d string) (time.Duration, error) {
	var hours string
	var mins string
	matches := offsetRegex.FindStringSubmatch(d)
	if len(matches) == 3 {
		hours = matches[1]
		mins = matches[2]
	} else {
		return -1, errors.New("Unable to process delta")
	}

	return time.ParseDuration(hours + "h" + mins + "m")
}

func (c *ConvertTz) Children() []sql.Expression {
	return []sql.Expression{c.dt, c.fromTz, c.toTz}
}

func (c *ConvertTz) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 3)
	}

	return NewConvertTz(ctx, children[0], children[1], children[2]), nil
}

func (c *ConvertTz) FunctionName() string {
	return "convert_tz"
}
