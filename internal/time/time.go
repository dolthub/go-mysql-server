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

// ConvertTimeZone converts |datetime| from one timezone to another. |fromLocation| and |toLocation|
// can be either the name of a timezone (e.g. "UTC") or a timezone offset (e.g. "+01:00"), but
// currently, both must be in the same format. If the time was converted successfully, then
// the second return value will be true, otherwise the time was not able to be converted.
func ConvertTimeZone(datetime time.Time, fromLocation string, toLocation string) (time.Time, bool) {
	// TODO: we can't currently deal with mixed use of timezone name (e.g. "UTC") and offset (e.g. "+0:00")
	converted, success := convertTimeZoneByLocationString(datetime, fromLocation, toLocation)
	if success {
		return converted, success
	}

	// If we weren't successful converting by timezone try converting via offsets.
	return convertTimeZoneByTimeOffset(datetime, fromLocation, toLocation)
}

// DeltaToDuration takes in a MySQL timezone offset (e.g. "+01:00") and returns it as a time.Duration.
// If any problems are encountered, an error is returned.
func DeltaToDuration(d string) (time.Duration, error) {
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

// SystemDelta returns the current system timezone offset as a MySQL timezone offset (e.g. "+01:00").
func SystemDelta() string {
	t := time.Now()
	_, offset := t.Zone()

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

// convertTimeZoneByLocationString returns the conversion of t from timezone fromLocation to toLocation.
func convertTimeZoneByLocationString(datetime time.Time, fromLocation string, toLocation string) (time.Time, bool) {
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

// convertTimeZoneByTimeOffset returns the conversion of t to t + (endDuration - startDuration) and a boolean indicating success.
func convertTimeZoneByTimeOffset(t time.Time, startDuration string, endDuration string) (time.Time, bool) {
	fromDuration, err := DeltaToDuration(startDuration)
	if err != nil {
		return time.Time{}, false
	}

	toDuration, err := DeltaToDuration(endDuration)
	if err != nil {
		return time.Time{}, false
	}

	return t.Add(toDuration - fromDuration), true
}
