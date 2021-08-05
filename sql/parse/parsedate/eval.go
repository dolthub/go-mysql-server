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
	var hour, minute, second, msecond int
	if dt.hours != nil {
		hour = int(*dt.hours)
	}
	if dt.minutes != nil {
		minute = int(*dt.minutes)
	}
	if dt.seconds != nil {
		second = int(*dt.seconds)
	}
	if dt.miliseconds != nil {
		msecond = int(*dt.miliseconds)
	}
	return time.Date(int(*dt.year), *dt.month, int(*dt.day), hour, minute, second, msecond, time.Local), nil
}
