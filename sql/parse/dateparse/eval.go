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

// Evaluate the parsed datetime params to a time.Time.
func evaluate(dt datetime) (time.Time, error) {
	err := validate(dt)
	if err != nil {
		return time.Time{}, err
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

	var (
		year, day int
		month     time.Month
	)
	if dt.year != nil {
		year = int(*dt.year)
	}
	if dt.dayOfYear != nil {
		// offset from Jan 1st by the specified number of days
		dayOffsetted := time.Date(year, time.January, 0, 0, 0, 0, 0, time.Local).AddDate(0, 0, int(*dt.dayOfYear))
		month = dayOffsetted.Month()
		day = dayOffsetted.Day()
	} else if dt.day != nil {
		month = *dt.month
		day = int(*dt.day)
	}

	// if timestamp only, add the duration to the 0 date
	if year == 0 && day == 0 && month == 0 {
		dur := time.Hour * time.Duration(hour)
		dur += time.Minute * time.Duration(minute)
		dur += time.Second*time.Duration(second) + nanosecondDuration
		return time.Time{}.Add(dur), nil
	}

	return time.Date(year, month, day, hour, minute, second, int(nanosecondDuration), time.Local), nil
}
