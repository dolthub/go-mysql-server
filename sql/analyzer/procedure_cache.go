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

package analyzer

import (
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql/plan"
)

// ProcedureCache contains all of the stored procedures for each database.
type ProcedureCache struct {
	dbToProcedureMap map[string]map[string]*plan.Procedure
	IsPopulating     bool
}

// NewProcedureCache returns a *ProcedureCache.
func NewProcedureCache() *ProcedureCache {
	return &ProcedureCache{
		dbToProcedureMap: make(map[string]map[string]*plan.Procedure),
		IsPopulating:     false,
	}
}

// Get returns the stored procedure with the given name from the given database. All names are case-insensitive. If the
// procedure does not exist, then this returns nil.
func (pc *ProcedureCache) Get(dbName, procedureName string) *plan.Procedure {
	dbName = strings.ToLower(dbName)
	procedureName = strings.ToLower(procedureName)
	if procMap, ok := pc.dbToProcedureMap[dbName]; ok {
		if procedure, ok := procMap[procedureName]; ok {
			return procedure
		}
	}
	return nil
}

// AllForDatabase returns all of the stored procedures for the given database, sorted by name ascending. The database
// name is case-insensitive.
func (pc *ProcedureCache) AllForDatabase(dbName string) []*plan.Procedure {
	dbName = strings.ToLower(dbName)
	var procedures []*plan.Procedure
	if procMap, ok := pc.dbToProcedureMap[dbName]; ok {
		procedures = make([]*plan.Procedure, len(procMap))
		i := 0
		for _, procedure := range procMap {
			procedures[i] = procedure
			i++
		}
		sort.Slice(procedures, func(i, j int) bool {
			return procedures[i].Name < procedures[j].Name
		})
	}
	return procedures
}

// Register adds the given stored procedure to the cache. Will overwrite any procedures that already exist with the
// same name for the given database name.
func (pc *ProcedureCache) Register(dbName string, procedure *plan.Procedure) {
	dbName = strings.ToLower(dbName)
	if procMap, ok := pc.dbToProcedureMap[dbName]; ok {
		procMap[strings.ToLower(procedure.Name)] = procedure
	} else {
		pc.dbToProcedureMap[dbName] = map[string]*plan.Procedure{strings.ToLower(procedure.Name): procedure}
	}
}
