package dateparse

import (
	"fmt"
	"time"
)

// Validate that the combination of fields in datetime
// can be evaluated unambiguously to a time.Time.
func validate(dt datetime) error {
	if dt.year == nil && dt.day == nil && dt.month == nil && dt.dayOfYear == nil {
		return nil
	}
	if dt.day == nil {
		if dt.month != nil {
			// TODO: ensure this behaves as expected
			return fmt.Errorf("day is ambiguous")
		}
		return nil
	}
	if dt.dayOfYear != nil && dt.day != nil {
		return fmt.Errorf("day is ambiguous")
	}
	if (dt.dayOfYear != nil || dt.day != nil) && dt.year == nil {
		return fmt.Errorf("year is ambiguous")
	}

	return nil
}

func evaluate(dt datetime, outType OutType) (interface{}, error) {
	if dt.isEmpty() {
		return nil, nil
	}

	var result string
	if outType == DateTime {
		d := getDate(dt)
		t := getTime(dt)
		result = d + " " + t
	} else if outType == TimeOnly {
		result = getTime(dt)
	} else if outType == DateOnly {
		result = getDate(dt)
	} else {
		return nil, nil
	}

	return result, nil
}

func getDate(dt datetime) string {
	var year, month, day int

	if dt.year != nil {
		year = int(*dt.year)
	}

	if dt.month != nil {
		month = int(*dt.month)
	}

	if dt.day != nil {
		day = int(*dt.day)
	}

	if dt.dayOfYear != nil {
		// offset from Jan 1st by the specified number of days
		dayOffsetted := time.Date(year, time.January, 0, 0, 0, 0, 0, time.Local).AddDate(0, 0, int(*dt.dayOfYear))
		month = int(dayOffsetted.Month())
		day = dayOffsetted.Day()
	}

	return fillWithZero(year, 4) + "-" + fillWithZero(month, 2) + "-" + fillWithZero(day, 2)
}

func getTime(dt datetime) string {
	var hours, minutes, seconds, milliseconds, microseconds, nanoseconds int

	if dt.hours != nil {
		if *dt.hours < 13 && dt.am != nil && !*dt.am {
			*dt.hours += 12
		}
		hours = int(*dt.hours)
	}
	if dt.minutes != nil {
		minutes = int(*dt.minutes)
	}
	if dt.seconds != nil {
		seconds = int(*dt.seconds)
	}

	t := fillWithZero(hours, 2) + ":" + fillWithZero(minutes, 2) + ":" + fillWithZero(seconds, 2)

	includeMicrosecond := false
	if dt.milliseconds != nil {
		milliseconds = int(*dt.milliseconds)
		includeMicrosecond = true
	}
	if dt.microseconds != nil {
		microseconds = int(*dt.microseconds)
		includeMicrosecond = true
	}
	if dt.nanoseconds != nil {
		nanoseconds = int(*dt.nanoseconds)
		includeMicrosecond = true
	}

	// convert partial seconds to nanoseconds
	nanosecondDuration := time.Microsecond*time.Duration(microseconds) + time.Millisecond*time.Duration(milliseconds) + time.Nanosecond*time.Duration(nanoseconds)
	if includeMicrosecond {
		t = t + "." + fillWithZero(int(nanosecondDuration), 6)
	}

	return t
}

func fillWithZero(n int, length int) string {
	r := fmt.Sprintf("%d", n)
	if len(r) > length {
		r = ""
	}
	for len(r) < length {
		r = fmt.Sprintf("0%s", r)
	}

	return r
}
