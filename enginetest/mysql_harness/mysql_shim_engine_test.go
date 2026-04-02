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

package mysql_harness

import (
	"os"
	"strconv"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
)

// These tests run engine tests of your choice against a live MySQL server to validate that enginetest
// expectations match actual MySQL behavior.
// They are destructive to data on that server, so  use with caution.
//
// To run these tests, start a MySQL instance and set the following environment variables:
//
//	MYSQL_TEST_USER     (default: "root")
//	MYSQL_TEST_PASSWORD (default: "")
//	MYSQL_TEST_HOST     (default: "localhost")
//	MYSQL_TEST_PORT     (default: "3306")
//
// Example:
//
//	MYSQL_TEST_HOST=localhost go test -run TestMySQL -v -count=1 ./enginetest/mysqlshim/

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func newMySQLHarnessForTests(t *testing.T) *MySQLHarness {
	t.Helper()
	if os.Getenv("MYSQL_TEST_HOST") == "" {
		t.Skip("skipping MySQL shim test: MYSQL_TEST_HOST not set")
	}

	user := getEnv("MYSQL_TEST_USER", "root")
	password := getEnv("MYSQL_TEST_PASSWORD", "")
	host := getEnv("MYSQL_TEST_HOST", "localhost")
	portStr := getEnv("MYSQL_TEST_PORT", "3306")
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("invalid MYSQL_TEST_PORT: %s", portStr)
	}

	harness, err := NewMySQLHarness(user, password, host, port)
	if err != nil {
		t.Fatalf("failed to create MySQL harness: %v", err)
	}
	t.Cleanup(func() { harness.Close() })
	return harness
}

func TestMySQLSpatialScripts(t *testing.T) {
	enginetest.TestSpatialScripts(t, newMySQLHarnessForTests(t))
}

func TestMySQLLargeGeometryScripts(t *testing.T) {
	enginetest.TestLargeGeometryScripts(t, newMySQLHarnessForTests(t))
}

func TestMySQLSpatialQueries(t *testing.T) {
	enginetest.TestSpatialQueries(t, newMySQLHarnessForTests(t))
}

func TestMySQLSpatialInsertInto(t *testing.T) {
	enginetest.TestSpatialInsertInto(t, newMySQLHarnessForTests(t))
}

func TestMySQLSpatialUpdate(t *testing.T) {
	enginetest.TestSpatialUpdate(t, newMySQLHarnessForTests(t))
}

func TestMySQLSpatialDelete(t *testing.T) {
	enginetest.TestSpatialDelete(t, newMySQLHarnessForTests(t))
}
