// Copyright 2025 Dolthub, Inc.
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

package function

import (
	regex "github.com/dolthub/go-icu-regex"
	"github.com/sirupsen/logrus"
)

func init() {
	// By default, the dolthub/go-icu-regex package will panic if a regex object is
	// finalized without having the Dispose() method called. Instead of panicing, we
	// add a RegexLeakHandler to log an error.
	regex.SetRegexLeakHandler(func() {
		logrus.Error("Detected leaked go-icu-regex.Regex instance")
	})
}
