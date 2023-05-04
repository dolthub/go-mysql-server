package inittime

import (
	"os"
	"time"
)

func init() {
	os.Setenv("TZ", "UTC")
	time.Local = time.UTC
}
