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
	"math"
	"sort"
	"strings"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

// ProcedureCache contains all of the stored procedures for each database.
type ProcedureCache struct {
	dbToProcedureMap map[string]map[string]map[int]*plan.Procedure
	mu               sync.RWMutex
}

// NewProcedureCache returns a *ProcedureCache.
func NewProcedureCache() *ProcedureCache {
	return &ProcedureCache{
		dbToProcedureMap: make(map[string]map[string]map[int]*plan.Procedure),
	}
}

// Get returns the stored procedure with the given name from the given database. All names are case-insensitive. If the
// procedure does not exist, then this returns nil. If the number of parameters do not match any given procedure, then
// returns the procedure with the largest number of parameters.
func (pc *ProcedureCache) Get(dbName, procedureName string, numOfParams int) *plan.Procedure {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	dbName = strings.ToLower(dbName)
	procedureName = strings.ToLower(procedureName)
	if procMap, ok := pc.dbToProcedureMap[dbName]; ok {
		if procedures, ok := procMap[procedureName]; ok {
			if procedure, ok := procedures[numOfParams]; ok {
				return procedure
			}

			var largestParamLen int
			var largestParamProc *plan.Procedure
			for _, procedure := range procedures {
				paramLen := len(procedure.Params)
				if procedure.HasVariadicParameter() {
					paramLen = math.MaxInt
				}
				if largestParamProc == nil || largestParamLen < paramLen {
					largestParamProc = procedure
					largestParamLen = paramLen
				}
			}
			return largestParamProc
		}
	}
	return nil
}

// AllForDatabase returns all of the stored procedures for the given database, sorted by name and parameter count
// ascending. The database name is case-insensitive.
func (pc *ProcedureCache) AllForDatabase(dbName string) []*plan.Procedure {
	pc.mu.RLock()
	defer pc.mu.RUnlock()

	dbName = strings.ToLower(dbName)
	var proceduresForDb []*plan.Procedure
	if procMap, ok := pc.dbToProcedureMap[dbName]; ok {
		for _, procedures := range procMap {
			for _, procedure := range procedures {
				proceduresForDb = append(proceduresForDb, procedure)
			}
		}
		sort.Slice(proceduresForDb, func(i, j int) bool {
			if proceduresForDb[i].Name != proceduresForDb[j].Name {
				return proceduresForDb[i].Name < proceduresForDb[j].Name
			}
			return len(proceduresForDb[i].Params) < len(proceduresForDb[j].Params)
		})
	}
	return proceduresForDb
}

// procedureSet holds stores a slice of Procedures for one or more databases.
type procedureSet map[string][]*plan.Procedure

// Register registers stored procedures.
func (pc *ProcedureCache) Register(ctx *sql.Context, cb func(ctx *sql.Context) (procedureSet, error)) (err error) {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	var set procedureSet
	if set, err = cb(ctx); err != nil {
		return err
	}

	// register all or none
	for db, slice := range set {
		db = strings.ToLower(db)

		for _, proc := range slice {
			card := len(proc.Params)
			name := strings.ToLower(proc.Name)

			if _, ok := pc.dbToProcedureMap[db]; !ok {
				continue
			}
			if _, ok := pc.dbToProcedureMap[db][name]; !ok {
				continue
			}
			if _, ok := pc.dbToProcedureMap[db][name][card]; ok {
				return sql.ErrExternalProcedureAmbiguousOverload.New(proc.Name, card)
			}
		}
	}

	for db, slice := range set {
		db = strings.ToLower(db)

		for _, proc := range slice {
			card := len(proc.Params)
			name := strings.ToLower(proc.Name)

			if _, ok := pc.dbToProcedureMap[db]; !ok {
				pc.dbToProcedureMap[db] = make(map[string]map[int]*plan.Procedure)
			}
			if _, ok := pc.dbToProcedureMap[db][name]; !ok {
				pc.dbToProcedureMap[db][name] = make(map[int]*plan.Procedure)
			}

			pc.dbToProcedureMap[db][name][card] = proc
		}
	}
	return nil
}
