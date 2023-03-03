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
)

// StoredProcedureDetails are the details of the stored procedure. Integrators only need to store and retrieve the given
// details for a stored procedure, as the engine handles all parsing and processing.
type StoredProcedureDetails struct {
	Name            string    // The name of this stored procedure. Names must be unique within a database.
	CreateStatement string    // The CREATE statement for this stored procedure.
	CreatedAt       time.Time // The time that the stored procedure was created.
	ModifiedAt      time.Time // The time of the last modification to the stored procedure.
}

// ExternalStoredProcedureDetails are the details of an external stored procedure. Compared to standard stored
// procedures, external ones are considered "built-in", in that they're not created by the user, and may not be modified
// or deleted by a user. In addition, they're implemented as a function taking standard parameters, compared to stored
// procedures being implemented as expressions.
type ExternalStoredProcedureDetails struct {
	// Name is the name of the external stored procedure. If two external stored procedures share a name, then they're
	// considered overloaded. Standard stored procedures do not support overloading.
	Name string
	// Schema describes the row layout of the RowIter returned from Function.
	Schema Schema
	// Function is the implementation of the external stored procedure. All functions should have the following definition:
	// `func(*Context, <PARAMETERS>) (RowIter, error)`. The <PARAMETERS> may be any of the following types: `bool`,
	// `string`, `[]byte`, `int8`-`int64`, `uint8`-`uint64`, `float32`, `float64`, `time.Time`, or `Decimal`
	// (shopspring/decimal). The architecture-dependent types `int` and `uint` (without a number) are also supported.
	// It is valid to return a nil RowIter if there are no rows to be returned.
	//
	// Each parameter, by default, is an IN parameter. If the parameter type is a pointer, e.g. `*int32`, then it
	// becomes an INOUT parameter. INOUT parameters will be given their zero value if the parameter's value is nil.
	// There is no way to set a parameter as an OUT parameter.
	//
	// Values are converted to their nearest type before being passed in, following the conversion rules of their
	// related SQL types. The exceptions are `time.Time` (treated as a `DATETIME`), string (treated as a `LONGTEXT` with
	// the default collation) and Decimal (treated with a larger precision and scale). Take extra care when using decimal
	// for an INOUT parameter, to ensure that the returned value fits the original's precision and scale, else an error
	// will occur.
	//
	// As functions support overloading, each variant must have a completely unique function signature to prevent
	// ambiguity. Uniqueness is determined by the number of parameters. If two functions are returned that have the same
	// name and same number of parameters, then an error is thrown. If the last parameter is variadic, then the stored
	// procedure functions as though it has the integer-max number of parameters. When an exact match is not found for
	// overloaded functions, the largest function is used (which in this case will be the variadic function). Also, due
	// to the usage of the integer-max for the parameter count, only one variadic function is allowed per function name.
	// The type of the variadic parameter may not have a pointer type.
	Function interface{}
}

// FakeCreateProcedureStmt returns a parseable CREATE PROCEDURE statement for this external stored procedure, as some
// tools (such as Java's JDBC connector) require a valid statement in some situations.
func (espd ExternalStoredProcedureDetails) FakeCreateProcedureStmt() string {
	return fmt.Sprintf("CREATE PROCEDURE %s() SELECT 'External stored procedure';", espd.Name)
}
