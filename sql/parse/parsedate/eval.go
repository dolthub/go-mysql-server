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
	return time.Date(int(*dt.year), *dt.month, int(*dt.day), 0, 0, 0, 0, time.Local), nil
}
