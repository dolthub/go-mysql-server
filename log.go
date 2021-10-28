// Copyright 2020-2021 Dolthub, Inc.
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

package sqle

import (
	vtlog "github.com/dolthub/vitess/go/vt/log"
	"github.com/golang/glog"
	"github.com/sirupsen/logrus"
)

const ConnectionIdLogField = "connectionID"
const ConnectTimeLogKey = "connectTime"

func init() {
	// V quickly checks if the logging verbosity meets a threshold.
	vtlog.V = func(level glog.Level) glog.Verbose {
		lvl := logrus.GetLevel()
		switch int32(level) {
		case 0:
			return glog.Verbose(lvl == logrus.InfoLevel)
		case 1:
			return glog.Verbose(lvl == logrus.WarnLevel)
		case 2:
			return glog.Verbose(lvl == logrus.ErrorLevel)
		case 3:
			return glog.Verbose(lvl == logrus.FatalLevel)
		default:
			return glog.Verbose(false)
		}
	}

	// Flush ensures any pending I/O is written.
	vtlog.Flush = func() {}

	// Info formats arguments like fmt.Print.
	vtlog.Info = logrus.Info
	// Infof formats arguments like fmt.Printf.
	vtlog.Infof = logrus.Infof
	// InfoDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	vtlog.InfoDepth = func(_ int, args ...interface{}) {
		logrus.Info(args...)
	}

	// Warning formats arguments like fmt.Print.
	vtlog.Warning = logrus.Warning
	// Warningf formats arguments like fmt.Printf.
	vtlog.Warningf = logrus.Warningf
	// WarningDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	vtlog.WarningDepth = func(depth int, args ...interface{}) {
		logrus.Warning(args...)
	}

	// Error formats arguments like fmt.Print.
	vtlog.Error = logrus.Error
	// Errorf formats arguments like fmt.Printf.
	vtlog.Errorf = logrus.Errorf
	// ErrorDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	vtlog.ErrorDepth = func(_ int, args ...interface{}) {
		logrus.Error(args...)
	}

	// Exit formats arguments like fmt.Print.
	vtlog.Exit = logrus.Panic
	// Exitf formats arguments like fmt.Printf.
	vtlog.Exitf = logrus.Panicf
	// ExitDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	vtlog.ExitDepth = func(_ int, args ...interface{}) {
		logrus.Panic(args...)
	}

	// Fatal formats arguments like fmt.Print.
	vtlog.Fatal = logrus.Fatal
	// Fatalf formats arguments like fmt.Printf
	vtlog.Fatalf = logrus.Fatalf
	// FatalDepth formats arguments like fmt.Print and uses depth to choose which call frame to log.
	vtlog.FatalDepth = func(_ int, args ...interface{}) {
		logrus.Fatal(args...)
	}
}
