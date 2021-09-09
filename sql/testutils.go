// Copyright 2021 Dolthub, Inc.
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

package sql

import (
	"encoding/json"
	"strings"

	"github.com/dolthub/go-mysql-server/internal/similartext"
)

func MustConvert(val interface{}, err error) interface{} {
	if err != nil {
		panic(err)
	}
	return val
}

func MustJSON(s string) JSONDocument {
	var doc interface{}
	if err := json.Unmarshal([]byte(s), &doc); err != nil {
		panic(err)
	}
	return JSONDocument{Val: doc}
}

// testProvider is a DatabaseProvider.
type testProvider struct {
	dbs []Database
}

var _ DatabaseProvider = testProvider{}

func NewTestProvider(dbs ...Database) DatabaseProvider {
	return testProvider{dbs: dbs}
}

// Database returns the Database with the given name if it exists.
func (d testProvider) Database(name string) (Database, error) {
	if len(d.dbs) == 0 {
		return nil, ErrDatabaseNotFound.New(name)
	}

	name = strings.ToLower(name)
	var dbNames []string
	for _, db := range d.dbs {
		if strings.ToLower(db.Name()) == name {
			return db, nil
		}
		dbNames = append(dbNames, db.Name())
	}
	similar := similartext.Find(dbNames, name)
	return nil, ErrDatabaseNotFound.New(name + similar)
}

// HasDatabase returns the Database with the given name if it exists.
func (d testProvider) HasDatabase(name string) bool {
	name = strings.ToLower(name)
	for _, db := range d.dbs {
		if strings.ToLower(db.Name()) == name {
			return true
		}
	}
	return false
}

// AllDatabases returns the Database with the given name if it exists.
func (d testProvider) AllDatabases() []Database {
	return d.dbs
}
