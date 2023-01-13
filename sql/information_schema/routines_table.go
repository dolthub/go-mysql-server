// Copyright 2022 Dolthub, Inc.
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

package information_schema

import (
	"bytes"
	"fmt"

	. "github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

type routineTable struct {
	name       string
	schema     Schema
	catalog    Catalog
	procedures map[string][]*plan.Procedure
	// functions
	rowIter func(*Context, Catalog, map[string][]*plan.Procedure) (RowIter, error)
}

var (
	_ Table = (*routineTable)(nil)
)

var doltProcedureAliasSet = map[string]interface{}{
	"dadd":                    nil,
	"dbranch":                 nil,
	"dcheckout":               nil,
	"dclean":                  nil,
	"dcommit":                 nil,
	"dfetch":                  nil,
	"dmerge":                  nil,
	"dpull":                   nil,
	"dpush":                   nil,
	"dreset":                  nil,
	"drevert":                 nil,
	"dverify_constraints":     nil,
	"dverify_all_constraints": nil,
}

func (t *routineTable) AssignCatalog(cat Catalog) Table {
	t.catalog = cat
	return t
}

func (r *routineTable) AssignProcedures(p map[string][]*plan.Procedure) Table {
	// TODO: should also assign functions
	r.procedures = p
	return r
}

// Name implements the sql.Table interface.
func (r *routineTable) Name() string {
	return r.name
}

// Schema implements the sql.Table interface.
func (r *routineTable) Schema() Schema {
	return r.schema
}

// Collation implements the sql.Table interface.
func (r *routineTable) Collation() CollationID {
	return Collation_Default
}

func (r *routineTable) String() string {
	return printTable(r.Name(), r.Schema())
}

func (r *routineTable) Partitions(context *Context) (PartitionIter, error) {
	return &informationSchemaPartitionIter{informationSchemaPartition: informationSchemaPartition{partitionKey(r.Name())}}, nil
}

func (r *routineTable) PartitionRows(context *Context, partition Partition) (RowIter, error) {
	if !bytes.Equal(partition.Key(), partitionKey(r.Name())) {
		return nil, ErrPartitionNotFound.New(partition.Key())
	}
	if r.rowIter == nil {
		return RowsToRowIter(), nil
	}
	if r.catalog == nil {
		return nil, fmt.Errorf("nil catalog for info schema table %s", r.name)
	}

	return r.rowIter(context, r.catalog, r.procedures)
}

func routinesRowIter(ctx *Context, c Catalog, p map[string][]*plan.Procedure) (RowIter, error) {
	var rows []Row
	var (
		securityType    string
		isDeterministic string
		sqlDataAccess   string
	)

	characterSetClient, err := ctx.GetSessionVariable(ctx, "character_set_client")
	if err != nil {
		return nil, err
	}
	collationConnection, err := ctx.GetSessionVariable(ctx, "collation_connection")
	if err != nil {
		return nil, err
	}
	collationServer, err := ctx.GetSessionVariable(ctx, "collation_server")
	if err != nil {
		return nil, err
	}

	sysVal, err := ctx.Session.GetSessionVariable(ctx, "sql_mode")
	if err != nil {
		return nil, err
	}
	sqlMode, ok := sysVal.(string)
	if !ok {
		return nil, ErrSystemVariableCodeFail.New("sql_mode", sysVal)
	}

	showExternalProcedures, err := ctx.GetSessionVariable(ctx, "show_external_procedures")
	if err != nil {
		return nil, err
	}
	for db, procedures := range p {
		for _, procedure := range procedures {
			// Skip dolt procedure aliases to show in this table
			if _, isAlias := doltProcedureAliasSet[procedure.Name]; isAlias {
				continue
			}
			// We skip external procedures if the variable to show them is set to false
			if showExternalProcedures.(int8) == 0 && procedure.IsExternal() {
				continue
			}

			parsedProcedure, err := parse.Parse(ctx, procedure.CreateProcedureString)
			if err != nil {
				return nil, err
			}
			procedurePlan, ok := parsedProcedure.(*plan.CreateProcedure)
			if !ok {
				return nil, ErrProcedureCreateStatementInvalid.New(procedure.CreateProcedureString)
			}
			routineDef := procedurePlan.BodyString

			securityType = "DEFINER"
			isDeterministic = "NO" // YES or NO
			sqlDataAccess = "CONTAINS SQL"
			for _, ch := range procedure.Characteristics {
				if ch == plan.Characteristic_LanguageSql {

				}

				if ch == plan.Characteristic_Deterministic {
					isDeterministic = "YES"
				} else if ch == plan.Characteristic_NotDeterministic {
					isDeterministic = "NO"
				}

				if ch == plan.Characteristic_ContainsSql {
					sqlDataAccess = "CONTAINS SQL"
				} else if ch == plan.Characteristic_NoSql {
					sqlDataAccess = "NO SQL"
				} else if ch == plan.Characteristic_ReadsSqlData {
					sqlDataAccess = "READS SQL DATA"
				} else if ch == plan.Characteristic_ModifiesSqlData {
					sqlDataAccess = "MODIFIES SQL DATA"
				}
			}

			if procedure.SecurityContext == plan.ProcedureSecurityContext_Invoker {
				securityType = "INVOKER"
			}
			rows = append(rows, Row{
				procedure.Name,             // specific_name NOT NULL
				"def",                      // routine_catalog
				db,                         // routine_schema
				procedure.Name,             // routine_name NOT NULL
				"PROCEDURE",                // routine_type NOT NULL
				"",                         // data_type
				nil,                        // character_maximum_length
				nil,                        // character_octet_length
				nil,                        // numeric_precision
				nil,                        // numeric_scale
				nil,                        // datetime_precision
				nil,                        // character_set_name
				nil,                        // collation_name
				"",                         // dtd_identifier
				"SQL",                      // routine_body NOT NULL
				routineDef,                 // routine_definition
				nil,                        // external_name
				"SQL",                      // external_language NOT NULL
				"SQL",                      // parameter_style NOT NULL
				isDeterministic,            // is_deterministic NOT NULL
				sqlDataAccess,              // sql_data_access NOT NULL
				nil,                        // sql_path
				securityType,               // security_type NOT NULL
				procedure.CreatedAt.UTC(),  // created NOT NULL
				procedure.ModifiedAt.UTC(), // last_altered NOT NULL
				sqlMode,                    // sql_mode NOT NULL
				procedure.Comment,          // routine_comment NOT NULL
				procedure.Definer,          // definer NOT NULL
				characterSetClient,         // character_set_client NOT NULL
				collationConnection,        // collation_connection NOT NULL
				collationServer,            // database_collation NOT NULL
			})
		}
	}

	// TODO: need to add FUNCTIONS routine_type

	return RowsToRowIter(rows...), nil
}
