// Copyright 2020-2021 Dolthub, Inc.
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

package expression

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	errors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/go-mysql-server/sql"
)

// Interval defines a time duration.
type Interval struct {
	UnaryExpression
	Unit string
}

// NewInterval creates a new interval expression.
func NewInterval(child sql.Expression, unit string) *Interval {
	return &Interval{UnaryExpression{Child: child}, strings.ToUpper(unit)}
}

// Type implements the sql.Expression interface.
func (i *Interval) Type() sql.Type { return sql.Uint64 }

// IsNullable implements the sql.Expression interface.
func (i *Interval) IsNullable() bool { return i.Child.IsNullable() }

// Eval implements the sql.Expression interface.
func (i *Interval) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	panic("Interval.Eval is just a placeholder method and should not be called directly")
}

var (
	errInvalidIntervalUnit   = errors.NewKind("invalid interval unit: %s")
	errInvalidIntervalFormat = errors.NewKind("invalid interval format for %q: %s")
)

// EvalDelta evaluates the expression returning a TimeDelta. This method should
// be used instead of Eval, as this expression returns a TimeDelta, which is not
// a valid value that can be returned in Eval.
func (i *Interval) EvalDelta(ctx *sql.Context, row sql.Row) (*TimeDelta, error) {
	val, err := i.Child.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	if val == nil {
		return nil, nil
	}

	var td TimeDelta

	if r, ok := unitTextFormats[i.Unit]; ok {
		val, err = sql.LongText.Convert(val)
		if err != nil {
			return nil, err
		}

		text := val.(string)
		if !r.MatchString(text) {
			return nil, errInvalidIntervalFormat.New(i.Unit, text)
		}

		parts := textFormatParts(text, r)

		switch i.Unit {
		case "DAY_HOUR":
			td.Days = parts[0]
			td.Hours = parts[1]
		case "DAY_MICROSECOND":
			td.Days = parts[0]
			td.Hours = parts[1]
			td.Minutes = parts[2]
			td.Seconds = parts[3]
			td.Microseconds = parts[4]
		case "DAY_MINUTE":
			td.Days = parts[0]
			td.Hours = parts[1]
			td.Minutes = parts[2]
		case "DAY_SECOND":
			td.Days = parts[0]
			td.Hours = parts[1]
			td.Minutes = parts[2]
			td.Seconds = parts[3]
		case "HOUR_MICROSECOND":
			td.Hours = parts[0]
			td.Minutes = parts[1]
			td.Seconds = parts[2]
			td.Microseconds = parts[3]
		case "HOUR_SECOND":
			td.Hours = parts[0]
			td.Minutes = parts[1]
			td.Seconds = parts[2]
		case "HOUR_MINUTE":
			td.Hours = parts[0]
			td.Minutes = parts[1]
		case "MINUTE_MICROSECOND":
			td.Minutes = parts[0]
			td.Seconds = parts[1]
			td.Microseconds = parts[2]
		case "MINUTE_SECOND":
			td.Minutes = parts[0]
			td.Seconds = parts[1]
		case "SECOND_MICROSECOND":
			td.Seconds = parts[0]
			td.Microseconds = parts[1]
		case "YEAR_MONTH":
			td.Years = parts[0]
			td.Months = parts[1]
		default:
			return nil, errInvalidIntervalUnit.New(i.Unit)
		}
	} else {
		val, err = sql.Int64.Convert(val)
		if err != nil {
			return nil, err
		}

		num := val.(int64)

		switch i.Unit {
		case "DAY":
			td.Days = num
		case "HOUR":
			td.Hours = num
		case "MINUTE":
			td.Minutes = num
		case "SECOND":
			td.Seconds = num
		case "MICROSECOND":
			td.Microseconds = num
		case "QUARTER":
			td.Months = num * 3
		case "MONTH":
			td.Months = num
		case "WEEK":
			td.Days = num * 7
		case "YEAR":
			td.Years = num
		default:
			return nil, errInvalidIntervalUnit.New(i.Unit)
		}
	}

	return &td, nil
}

// WithChildren implements the Expression interface.
func (i *Interval) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
	if len(children) != 1 {
		return nil, sql.ErrInvalidChildrenNumber.New(i, len(children), 1)
	}
	return NewInterval(children[0], i.Unit), nil
}

func (i *Interval) String() string {
	return fmt.Sprintf("INTERVAL %s %s", i.Child, i.Unit)
}

var unitTextFormats = map[string]*regexp.Regexp{
	"DAY_HOUR":           regexp.MustCompile(`^(\d+)\s+(\d+)$`),
	"DAY_MICROSECOND":    regexp.MustCompile(`^(\d+)\s+(\d+):(\d+):(\d+).(\d+)$`),
	"DAY_MINUTE":         regexp.MustCompile(`^(\d+)\s+(\d+):(\d+)$`),
	"DAY_SECOND":         regexp.MustCompile(`^(\d+)\s+(\d+):(\d+):(\d+)$`),
	"HOUR_MICROSECOND":   regexp.MustCompile(`^(\d+):(\d+):(\d+).(\d+)$`),
	"HOUR_SECOND":        regexp.MustCompile(`^(\d+):(\d+):(\d+)$`),
	"HOUR_MINUTE":        regexp.MustCompile(`^(\d+):(\d+)$`),
	"MINUTE_MICROSECOND": regexp.MustCompile(`^(\d+):(\d+).(\d+)$`),
	"MINUTE_SECOND":      regexp.MustCompile(`^(\d+):(\d+)$`),
	"SECOND_MICROSECOND": regexp.MustCompile(`^(\d+).(\d+)$`),
	"YEAR_MONTH":         regexp.MustCompile(`^(\d+)-(\d+)$`),
}

func textFormatParts(text string, r *regexp.Regexp) []int64 {
	parts := r.FindStringSubmatch(text)
	var result []int64
	for _, p := range parts[1:] {
		// It is safe to igore the error here, because at this point we know
		// the string matches the regexp, and that means it can't be an
		// invalid number.
		n, _ := strconv.ParseInt(p, 10, 64)
		result = append(result, n)
	}
	return result
}

// TimeDelta is the difference between a time and another time.
type TimeDelta struct {
	Years        int64
	Months       int64
	Days         int64
	Hours        int64
	Minutes      int64
	Seconds      int64
	Microseconds int64
}

// Add returns the given time plus the time delta.
func (td TimeDelta) Add(t time.Time) time.Time {
	return td.apply(t, 1)
}

// Sub returns the given time minus the time delta.
func (td TimeDelta) Sub(t time.Time) time.Time {
	return td.apply(t, -1)
}

const (
	day  = 24 * time.Hour
	week = 7 * day
)

func (td TimeDelta) apply(t time.Time, sign int64) time.Time {
	y := int64(t.Year())
	mo := int64(t.Month())
	d := t.Day()
	h := t.Hour()
	min := t.Minute()
	s := t.Second()
	ns := t.Nanosecond()

	if td.Years != 0 {
		y += td.Years * sign
	}

	if td.Months != 0 {
		m := mo + td.Months*sign
		if m < 1 {
			mo = 12 + (m % 12)
			y += m/12 - 1
		} else if m > 12 {
			mo = m % 12
			y += m / 12
		} else {
			mo = m
		}

		// Due to the operations done before, month may be zero, which means it's
		// december.
		if mo == 0 {
			mo = 12
		}
	}

	if days := daysInMonth(time.Month(mo), int(y)); days < d {
		d = days
	}

	date := time.Date(int(y), time.Month(mo), d, h, min, s, ns, t.Location())

	if td.Days != 0 {
		date = date.Add(time.Duration(td.Days) * day * time.Duration(sign))
	}

	if td.Hours != 0 {
		date = date.Add(time.Duration(td.Hours) * time.Hour * time.Duration(sign))
	}

	if td.Minutes != 0 {
		date = date.Add(time.Duration(td.Minutes) * time.Minute * time.Duration(sign))
	}

	if td.Seconds != 0 {
		date = date.Add(time.Duration(td.Seconds) * time.Second * time.Duration(sign))
	}

	if td.Microseconds != 0 {
		date = date.Add(time.Duration(td.Microseconds) * time.Microsecond * time.Duration(sign))
	}

	return date
}

func daysInMonth(month time.Month, year int) int {
	if month == time.December {
		return 31
	}

	date := time.Date(year, month+time.Month(1), 1, 0, 0, 0, 0, time.Local)
	return date.Add(-1 * day).Day()
}
