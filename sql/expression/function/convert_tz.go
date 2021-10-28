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

var offsetRegex = regexp.MustCompile(`(?m)^(\+|\-)(\d{2}):(\d{2})$`) // (?m)^\+|\-(\d{2}):(\d{2})$

type ConvertTz struct {
	dt     sql.Expression
	fromTz sql.Expression
	toTz   sql.Expression
}

var _ sql.FunctionExpression = (*ConvertTz)(nil)

// NewConvertTz returns an implementation of the CONVERT_TZ() function.
func NewConvertTz(dt, fromTz, toTz sql.Expression) sql.Expression {
	return &ConvertTz{
		dt:     dt,
		fromTz: fromTz,
		toTz:   toTz,
	}
}

// Resolved implements the sql.Expression interface.
func (c *ConvertTz) Resolved() bool {
	return c.dt.Resolved() && c.fromTz.Resolved() && c.toTz.Resolved()
}

// String implements the sql.Expression interface.
func (c *ConvertTz) String() string {
	return fmt.Sprintf("CONVERT_TZ(%s, %s, %s)", c.dt, c.fromTz, c.toTz)
}

// Type implements the sql.Expression interface.
func (c *ConvertTz) Type() sql.Type {
	return sql.Datetime
}

// IsNullable implements the sql.Expression interface.
func (c *ConvertTz) IsNullable() bool {
	return true
}

// Eval implements the sql.Expression interface.
func (c *ConvertTz) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	from, err := c.fromTz.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	to, err := c.toTz.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	dt, err := c.dt.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	// If either the date, or the timezones/offsets are not correct types we return NULL.
	datetime, err := sql.Datetime.ConvertWithoutRangeCheck(dt)
	if err != nil {
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

	converted, success := convertTimeZone(datetime, fromStr, toStr)
	if success {
		return sql.Datetime.ConvertWithoutRangeCheck(converted)
	}

	// If we weren't successful converting by timezone try converting via offsets.
	converted, success = convertOffsets(datetime, fromStr, toStr)
	if !success {
		return nil, nil
	}

	return sql.Datetime.ConvertWithoutRangeCheck(converted)
}

// convertTimeZone returns the conversion of t from timezone fromLocation to toLocation.
func convertTimeZone(datetime time.Time, fromLocation string, toLocation string) (time.Time, bool) {
	fLoc, err := time.LoadLocation(fromLocation)
	if err != nil {
		return time.Time{}, false
	}

	tLoc, err := time.LoadLocation(toLocation)
	if err != nil {
		return time.Time{}, false
	}

	delta := getCopy(datetime, fLoc).Sub(getCopy(datetime, tLoc))

	return datetime.Add(delta), true
}

// getCopy recreates the time t in the wanted timezone.
func getCopy(t time.Time, loc *time.Location) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc).UTC()
}

// convertOffsets returns the conversion of t to t + (endDuration - startDuration) and a boolean indicating success.
func convertOffsets(t time.Time, startDuration string, endDuration string) (time.Time, bool) {
	fromDuration, err := getDeltaAsDuration(startDuration)
	if err != nil {
		return time.Time{}, false
	}

	toDuration, err := getDeltaAsDuration(endDuration)
	if err != nil {
		return time.Time{}, false
	}

	return t.Add(toDuration - fromDuration), true
}

// getDeltaAsDuration takes in a MySQL offset in the format (ex +01:00) and returns it as a time Duration.
func getDeltaAsDuration(d string) (time.Duration, error) {
	var hours string
	var mins string
	var symbol string
	matches := offsetRegex.FindStringSubmatch(d)
	if len(matches) == 4 {
		symbol = matches[1]
		hours = matches[2]
		mins = matches[3]
	} else {
		return -1, errors.New("error: unable to process time")
	}

	return time.ParseDuration(symbol + hours + "h" + mins + "m")
}

// Children implements the sql.Expression interface.
func (c *ConvertTz) Children() []sql.Expression {
	return []sql.Expression{c.dt, c.fromTz, c.toTz}
}

// WithChildren implements the sql.Expression interface.
func (c *ConvertTz) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 3 {
		return nil, sql.ErrInvalidChildrenNumber.New(c, len(children), 3)
	}

	return NewConvertTz(children[0], children[1], children[2]), nil
}

// FunctionName implement the sql.FunctionExpression interface.
func (c *ConvertTz) FunctionName() string {
	return "convert_tz"
}
