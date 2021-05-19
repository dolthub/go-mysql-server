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

package main

import (
	"encoding/csv"
	"fmt"
	"os"

	"github.com/dolthub/sqllogictest/go/logictest"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/sqllogictest/harness"
)

type MemoryResultRecord struct {
	TestFile     string
	LineNum      int
	Query        string
	Duration     int64
	Result       string
	ErrorMessage string
}

func main() {
	args := os.Args[1:]

	if len(args) < 1 {
		panic("Usage: logictest (run|parse) file1 file2 ...")
	}

	if args[0] == "run" {
		h := harness.NewMemoryHarness(enginetest.NewDefaultMemoryHarness())
		logictest.RunTestFiles(h, args[1:]...)
	} else if args[0] == "parse" {
		if len(args) < 2 {
			panic("Usage: logictest parse (file | dir/)")
		}
		parseTestResults(args[1])
	} else {
		panic("Unrecognized command " + args[0])
	}
}

func parseTestResults(f string) {
	entries, err := logictest.ParseResultFile(f)
	if err != nil {
		panic(err)
	}

	records := make([]*MemoryResultRecord, len(entries))
	for i, e := range entries {
		records[i] = newMemoryRecordResult(e)
	}

	err = writeResultsCsv(records)
	if err != nil {
		panic(err)
	}
}

// fromResultCsvHeaders returns supported csv headers for a Result
func fromResultCsvHeaders() []string {
	return []string{
		"test_file",
		"line_num",
		"query_string",
		"duration",
		"result",
		"error_message",
	}
}

// writeResultsCsv writes []*MemoryResultRecord to stdout in csv format
func writeResultsCsv(results []*MemoryResultRecord) (err error) {
	csvWriter := csv.NewWriter(os.Stdout)

	// write header
	headers := fromResultCsvHeaders()
	if err := csvWriter.Write(headers); err != nil {
		return err
	}

	// write rows
	for _, r := range results {
		row := make([]string, 0)
		for _, field := range headers {
			val, err := fromHeaderColumnValue(field, r)
			if err != nil {
				return err
			}
			row = append(row, val)
		}
		err = csvWriter.Write(row)
		if err != nil {
			return err
		}
	}

	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return err
	}
	return
}

func newMemoryRecordResult(e *logictest.ResultLogEntry) *MemoryResultRecord {
	var result string
	switch e.Result {
	case logictest.Ok:
		result = "ok"
	case logictest.NotOk:
		result = "not ok"
	case logictest.Skipped:
		result = "skipped"
	case logictest.Timeout:
		result = "timeout"
	case logictest.DidNotRun:
		result = "did not run"
	}
	return &MemoryResultRecord{
		TestFile:     e.TestFile,
		LineNum:      e.LineNum,
		Query:        e.Query,
		Duration:     e.Duration.Milliseconds(),
		Result:       result,
		ErrorMessage: e.ErrorMessage,
	}
}

// fromHeaderColumnValue returns the value from the DoltResultRecord for the given
// header field
func fromHeaderColumnValue(h string, r *MemoryResultRecord) (string, error) {
	var val string
	switch h {
	case "test_file":
		val = r.TestFile
	case "line_num":
		val = fmt.Sprintf("%d", r.LineNum)
	case "query_string":
		val = r.Query
	case "duration":
		val = fmt.Sprintf("%d", r.Duration)
	case "result":
		val = r.Result
	case "error_message":
		val = r.ErrorMessage
	default:
		return "", fmt.Errorf("unsupported header field")
	}
	return val, nil
}
