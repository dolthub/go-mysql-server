// Copyright 2020 Dolthub, Inc.
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

package main

import (
	"os"

	"github.com/dolthub/sqllogictest/go/logictest"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/sqllogictest/harness"
)

func main() {
	args := os.Args[1:]

	if len(args) < 1 {
		panic("Usage: logictest (run|parse) [version] file1 file2 ...")
	}

	if args[0] == "run" {
		h := harness.NewMemoryHarness(enginetest.NewDefaultMemoryHarness())
		logictest.RunTestFiles(h, args[1:]...)
	} else {
		panic("Unrecognized command " + args[0])
	}
}
