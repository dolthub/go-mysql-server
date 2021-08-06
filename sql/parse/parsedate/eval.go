package parsedate

import (
	"fmt"
	"time"
)

// TODO: make this return a sql type directly, (then choose between date/datetime/timestamp)
func evaluate(dt datetime) (time.Time, error) {
	if dt.year == nil || dt.month == nil || dt.day == nil {
		return time.Time{}, fmt.Errorf("ambiguous datetime specification")
	}
	var hour, minute, second, miliseconds, microseconds, nanoseconds int
	if dt.hours != nil {
		if *dt.hours < 13 && dt.am != nil && !*dt.am {
			*dt.hours += 12
		}
		hour = int(*dt.hours)
	}
	if dt.minutes != nil {
		minute = int(*dt.minutes)
	}
	if dt.seconds != nil {
		second = int(*dt.seconds)
	}
	if dt.miliseconds != nil {
		miliseconds = int(*dt.miliseconds)
	}
	if dt.microseconds != nil {
		microseconds = int(*dt.microseconds)
	}
	if dt.nanoseconds != nil {
		nanoseconds = int(*dt.nanoseconds)
	}
	// convert partial seconds to nanoseconds
	nanosecondDuration := time.Microsecond*time.Duration(microseconds) + time.Millisecond*time.Duration(miliseconds) + time.Nanosecond*time.Duration(nanoseconds)

	return time.Date(int(*dt.year), *dt.month, int(*dt.day), hour, minute, second, int(nanosecondDuration), time.Local), nil
}
