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

package sql

import (
	"fmt"
	"time"

	"github.com/dolthub/vitess/go/vt/sqlparser"
)

// StatementRunner is essentially an interface that the engine will implement. We cannot directly reference the engine
// here as it will cause an import cycle, so this may be updated to suit any function changes that the engine
// experiences.
type StatementRunner interface {
	QueryWithBindings(ctx *Context, query string, parsed sqlparser.Statement, bindings map[string]sqlparser.Expr, qFlags *QueryFlags) (Schema, RowIter, *QueryFlags, error)
}

// StoredProcedureDetails are the details of the stored procedure. Integrators only need to store and retrieve the given
// details for a stored procedure, as the engine handles all parsing and processing.
type StoredProcedureDetails struct {
	Name            string    // The name of this stored procedure. Names must be unique within a database.
	CreateStatement string    // The CREATE statement for this stored procedure.
	CreatedAt       time.Time // The time that the stored procedure was created.
	ModifiedAt      time.Time // The time of the last modification to the stored procedure.
	SqlMode         string    // The SQL_MODE when this procedure was defined.
	SchemaName      string    // The name of the schema that this stored procedure belongs to, for databases that support schemas.
}

// ExternalStoredProcedureDetails are the details of an external stored procedure. Compared to standard stored
// procedures, external ones are considered "built-in", in that they're not created by the user, and may not be modified
// or deleted by a user. In addition, they're implemented as a function taking standard parameters, compared to stored
// procedures being implemented as expressions.
type ExternalStoredProcedureDetails struct {
	Function  interface{}
	Name      string
	Schema    Schema
	ReadOnly  bool
	AdminOnly bool
}

// FakeCreateProcedureStmt returns a parseable CREATE PROCEDURE statement for this external stored procedure, as some
// tools (such as Java's JDBC connector) require a valid statement in some situations.
func (espd ExternalStoredProcedureDetails) FakeCreateProcedureStmt() string {
	return fmt.Sprintf("CREATE PROCEDURE %s() SELECT 'External stored procedure';", espd.Name)
}
