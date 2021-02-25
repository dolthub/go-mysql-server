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
	"fmt"
	"strings"
)

// ProcedureSecurityContext determines whether the stored procedure is executed using the privileges of the definer or
// the invoker.
type ProcedureSecurityContext byte
const (
	// ProcedureSecurityContext_Definer uses the definer's security context.
	ProcedureSecurityContext_Definer ProcedureSecurityContext = iota
	// ProcedureSecurityContext_Invoker uses the invoker's security context.
	ProcedureSecurityContext_Invoker
)

// ProcedureParamDirection represents the use case of the stored procedure parameter.
type ProcedureParamDirection byte
const (
	// ProcedureParamDirection_In means the parameter passes its contained value to the stored procedure.
	ProcedureParamDirection_In ProcedureParamDirection = iota
	// ProcedureParamDirection_Inout means the parameter passes its contained value to the stored procedure, while also
	// modifying the given variable.
	ProcedureParamDirection_Inout
	// ProcedureParamDirection_Out means the parameter variable will be modified, but will not be read from within the
	// stored procedure.
	ProcedureParamDirection_Out
)

// ProcedureParam represents the parameter of a stored procedure.
type ProcedureParam struct {
	Direction ProcedureParamDirection // Direction is the direction of the parameter.
	Name      string // Name is the name of the parameter.
	Type      Type // Type is the SQL type of the parameter.
}

// Characteristic represents a characteristic that is defined on either a stored procedure or stored function.
type Characteristic byte
const (
	Characteristic_LanguageSql Characteristic = iota
	Characteristic_Deterministic
	Characteristic_NotDeterministic
	Characteristic_ContainsSql
	Characteristic_NoSql
	Characteristic_ReadsSqlData
	Characteristic_ModifiesSqlData
)

// ProcedureCache contains all of the stored procedures for each database.
type ProcedureCache struct {
	dbToProcedureMap map[string]map[string]*Procedure
	isPopulating     bool
}

// NewProcedureCache returns a *ProcedureCache. If you will be adding procedures immediately to the cache, then set
// the parameter to true.
func NewProcedureCache(willImmediatelyRegister bool) *ProcedureCache {
	return &ProcedureCache{
		dbToProcedureMap: make(map[string]map[string]*Procedure),
		isPopulating:     willImmediatelyRegister,
	}
}

// Get returns the stored procedure with the given name from the given database. All names are case-insensitive. If the
// procedure does not exist, then this returns nil.
func (pc *ProcedureCache) Get(dbName, procedureName string) *Procedure {
	pc.isPopulating = false
	dbName = strings.ToLower(dbName)
	procedureName = strings.ToLower(procedureName)
	if procMap, ok := pc.dbToProcedureMap[dbName]; ok {
		if procedure, ok := procMap[procedureName]; ok {
			return procedure
		}
	}
	return nil
}

// Register adds the given stored procedure to the cache. Will overwrite any procedures that already exist with the
// same name for the given database name.
func (pc *ProcedureCache) Register(dbName string, procedure *Procedure) {
	pc.isPopulating = true
	dbName = strings.ToLower(dbName)
	if procMap, ok := pc.dbToProcedureMap[dbName]; ok {
		procMap[strings.ToLower(procedure.Name)] = procedure
	} else {
		pc.dbToProcedureMap[dbName] = map[string]*Procedure{strings.ToLower(procedure.Name): procedure}
	}
}

// IsPopulating returns whether the cache is being populated with procedures.
func (pc *ProcedureCache) IsPopulating() bool {
	if pc == nil {
		return false
	}
	return pc.isPopulating
}

// Procedure is a stored procedure that may be executed using the CALL statement.
type Procedure struct {
	Name                  string
	Definer               string
	Params                []ProcedureParam
	SecurityContext       ProcedureSecurityContext
	Characteristics       []Characteristic
	CreateProcedureString string
	Body                  Node
}

// NewProcedure returns a *Procedure. All names contained within are lowercase, and all methods are case-insensitive.
func NewProcedure(
	name string,
	definer string,
	params []ProcedureParam,
	securityContext ProcedureSecurityContext,
	characteristics []Characteristic,
	createProcedureString string,
	body Node,
) *Procedure {
	lowercasedParams := make([]ProcedureParam, len(params))
	for i, param := range params {
		lowercasedParams[i] = ProcedureParam{
			Direction: param.Direction,
			Name:      strings.ToLower(param.Name),
			Type:      param.Type,
		}
	}
	return &Procedure{
		Name:                  strings.ToLower(name),
		Definer:               definer,
		Params:                lowercasedParams,
		SecurityContext:       securityContext,
		Characteristics:       characteristics,
		CreateProcedureString: createProcedureString,
		Body:                  body,
	}
}

// ParamIndex returns the index of the parameter with the given name. The name is case-insensitive. If the parameter
// does not exist, returns -1.
func (p *Procedure) ParamIndex(name string) int {
	name = strings.ToLower(name)
	for i, param := range p.Params {
		if param.Name == name {
			return i
		}
	}
	return -1
}

// String returns the original SQL representation.
func (pst ProcedureSecurityContext) String() string {
	switch pst {
	case ProcedureSecurityContext_Definer:
		return "SQL SECURITY DEFINER"
	case ProcedureSecurityContext_Invoker:
		return "SQL SECURITY INVOKER"
	default:
		panic(fmt.Errorf("invalid security context value `%d`", byte(pst)))
	}
}

// String returns the original SQL representation.
func (pp ProcedureParam) String() string {
	direction := ""
	switch pp.Direction {
	case ProcedureParamDirection_In:
		direction = "IN"
	case ProcedureParamDirection_Inout:
		direction = "INOUT"
	case ProcedureParamDirection_Out:
		direction = "OUT"
	}
	return fmt.Sprintf("%s %s %s", direction, pp.Name, pp.Type.String())
}

// String returns the original SQL representation.
func (c Characteristic) String() string {
	switch c {
	case Characteristic_LanguageSql:
		return "LANGUAGE SQL"
	case Characteristic_Deterministic:
		return "DETERMINISTIC"
	case Characteristic_NotDeterministic:
		return "NOT DETERMINISTIC"
	case Characteristic_ContainsSql:
		return "CONTAINS SQL"
	case Characteristic_NoSql:
		return "NO SQL"
	case Characteristic_ReadsSqlData:
		return "READS SQL DATA"
	case Characteristic_ModifiesSqlData:
		return "MODIFIES SQL DATA"
	default:
		panic(fmt.Errorf("invalid characteristic value `%d`", byte(c)))
	}
}
