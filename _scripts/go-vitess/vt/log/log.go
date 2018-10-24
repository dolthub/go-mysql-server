// You can modify this file to hook up a different logging library instead of logrus.
// If you adapt to a different logging framework, you may need to use that
// framework's equivalent of *Depth() functions so the file and line number printed
// point to the real caller instead of your adapter function.

package log

import "github.com/sirupsen/logrus"

// Level is used with V() to test log verbosity.
type Level = logrus.Level

var (
	// V quickly checks if the logging verbosity meets a threshold.
	V = func(level int) bool {
		lvl := logrus.GetLevel()
		switch level {
		case 0:
			return lvl == logrus.InfoLevel
		case 1:
			return lvl == logrus.WarnLevel
		case 2:
			return lvl == logrus.ErrorLevel
		case 3:
			return lvl == logrus.FatalLevel
		default:
			return false
		}
	}

	// Flush ensures any pending I/O is written.
	Flush = func() {}

	// Info formats arguments like fmt.Print.
	Info = logrus.Info
	// Infof formats arguments like fmt.Printf.
	Infof = logrus.Infof
	// InfoDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	InfoDepth = func(_ int, args ...interface{}) {
		logrus.Info(args...)
	}

	// Warning formats arguments like fmt.Print.
	Warning = logrus.Warning
	// Warningf formats arguments like fmt.Printf.
	Warningf = logrus.Warningf
	// WarningDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	WarningDepth = func(depth int, args ...interface{}) {
		logrus.Warning(args...)
	}

	// Error formats arguments like fmt.Print.
	Error = logrus.Error
	// Errorf formats arguments like fmt.Printf.
	Errorf = logrus.Errorf
	// ErrorDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	ErrorDepth = func(_ int, args ...interface{}) {
		logrus.Error(args...)
	}

	// Exit formats arguments like fmt.Print.
	Exit = logrus.Panic
	// Exitf formats arguments like fmt.Printf.
	Exitf = logrus.Panicf
	// ExitDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	ExitDepth = func(_ int, args ...interface{}) {
		logrus.Panic(args...)
	}

	// Fatal formats arguments like fmt.Print.
	Fatal = logrus.Fatal
	// Fatalf formats arguments like fmt.Printf
	Fatalf = logrus.Fatalf
	// FatalDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	FatalDepth = func(_ int, args ...interface{}) {
		logrus.Fatal(args...)
	}
)
