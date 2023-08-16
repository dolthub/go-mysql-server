package dateparse

import (
	"fmt"
	"time"
)

func evaluateDate(dt datetime) string {
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

func evaluateTime(dt datetime) string {
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
