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
	"sync"

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

// testProvider is a collection of Database.
type testProvider struct {
	dbs map[string]Database
	mu  *sync.RWMutex
}

var _ DatabaseProvider = testProvider{}

func NewTestProvider(dbs ...Database) DatabaseProvider {
	dbMap := make(map[string]Database, len(dbs))
	for _, db := range dbs {
		dbMap[strings.ToLower(db.Name())] = db
	}
	return testProvider{
		dbs: dbMap,
		mu:  &sync.RWMutex{},
	}
}

// Database returns the Database with the given name if it exists.
func (d testProvider) Database(name string) (Database, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	db, ok := d.dbs[strings.ToLower(name)]
	if ok {
		return db, nil
	}

	names := make([]string, 0, len(d.dbs))
	for n := range d.dbs {
		names = append(names, n)
	}

	similar := similartext.Find(names, name)
	return nil, ErrDatabaseNotFound.New(name + similar)
}

// HasDatabase returns the Database with the given name if it exists.
func (d testProvider) HasDatabase(name string) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()

	_, ok := d.dbs[strings.ToLower(name)]
	return ok
}

// AllDatabases returns the Database with the given name if it exists.
func (d testProvider) AllDatabases() []Database {
	d.mu.RLock()
	defer d.mu.RUnlock()

	all := make([]Database, 0, len(d.dbs))
	for _, db := range d.dbs {
		all = append(all, db)
	}
	return all
}
