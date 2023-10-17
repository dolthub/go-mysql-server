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

// Package time contains low-level utility functions for working with time.Time values and timezones.
package time

import (
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"
)

// offsetRegex is a regex for matching MySQL offsets (e.g. +01:00).
var offsetRegex = regexp.MustCompile(`(?m)^([+\-])(\d{2}):(\d{2})$`)

// ConvertTimeZone converts |datetime| from one timezone to another. |fromLocation| and |toLocation| can be either
// the name of a timezone (e.g. "UTC") or a MySQL-formatted timezone offset (e.g. "+01:00"). If the time was converted
// successfully, then the second return value will be true, otherwise the time was not able to be converted.
func ConvertTimeZone(datetime time.Time, fromLocation string, toLocation string) (time.Time, bool) {
	convertedFromTime, err := ConvertTimeToLocation(datetime, fromLocation)
	if err != nil {
		return time.Time{}, false
	}
	convertedToTime, err := ConvertTimeToLocation(datetime, toLocation)
	if err != nil {
		return time.Time{}, false
	}

	delta := convertedFromTime.Sub(convertedToTime)
	return datetime.Add(delta), true
}

// MySQLOffsetToDuration takes in a MySQL timezone offset (e.g. "+01:00") and returns it as a time.Duration.
// If any problems are encountered, an error is returned.
func MySQLOffsetToDuration(d string) (time.Duration, error) {
	matches := offsetRegex.FindStringSubmatch(d)
	if len(matches) == 4 {
		symbol := matches[1]
		hours := matches[2]
		mins := matches[3]
		return time.ParseDuration(symbol + hours + "h" + mins + "m")
	} else {
		return -1, errors.New("error: unable to process time")
	}
}

// SystemTimezoneOffset returns the current system timezone offset as a MySQL timezone offset (e.g. "+01:00").
func SystemTimezoneOffset() string {
	t := time.Now()
	_, offset := t.Zone()

	return SecondsToMySQLOffset(offset)
}

// SystemTimezoneName returns the current system timezone name.
func SystemTimezoneName() string {
	t := time.Now()
	name, _ := t.Zone()

	return name
}

// SecondsToMySQLOffset takes in a timezone offset in seconds (as returned by time.Time.Zone()) and returns it as a
// MySQL timezone offset (e.g. "+01:00").
func SecondsToMySQLOffset(offset int) string {
	seconds := offset % (60 * 60 * 24)
	hours := math.Floor(float64(seconds) / 60 / 60)
	seconds = offset % (60 * 60)
	minutes := math.Floor(float64(seconds) / 60)

	result := fmt.Sprintf("%02d:%02d", int(math.Abs(hours)), int(math.Abs(minutes)))
	if offset >= 0 {
		result = fmt.Sprintf("+%s", result)
	} else {
		result = fmt.Sprintf("-%s", result)
	}

	return result
}

// ConvertTimeToLocation converts |datetime| to the given |location|. |location| can be either the name of a timezone
// (e.g. "UTC") or a MySQL-formatted timezone offset (e.g. "+01:00"). If the time was converted successfully, then
// the converted time is returned, otherwise an error is returned.
func ConvertTimeToLocation(datetime time.Time, location string) (time.Time, error) {
	// Try to load the timezone location string first
	loc, err := time.LoadLocation(location)
	if err == nil {
		return getCopy(datetime, loc), nil
	}

	// If we can't parse a timezone location string, then try to parse a MySQL location offset
	duration, err := MySQLOffsetToDuration(location)
	if err == nil {
		return datetime.Add(-1 * duration), nil
	}

	return time.Time{}, errors.New(fmt.Sprintf("error: unable to parse timezone '%s'", location))
}

// getCopy recreates the time t in the wanted timezone.
func getCopy(t time.Time, loc *time.Location) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), loc).UTC()
}
