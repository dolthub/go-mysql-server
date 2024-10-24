// Copyright 2024 Dolthub, Inc.
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

package planbuilder

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
)

// AuthorizationHandler handles the authorization of queries, generally through the use of a privilege system. This
// handler exists to create handlers that will operate on a single query.
type AuthorizationHandler interface {
	// NewQueryHandler returns a new QueryAuthorizationHandler that will be used for the authorization of a single SQL
	// query, after which it is discarded. The query handler should retain any information that may be used to authorize
	// queries, such as the context, databases, etc.
	NewQueryHandler(ctx *sql.Context, cat sql.Catalog) (QueryAuthorizationHandler, error)
}

// QueryAuthorizationHandler handles the authorization of a single query.
type QueryAuthorizationHandler interface {
	// Handle checks whether the given information is valid based on the state retained from the handler's creation. For
	// example, whether a user has a specific privilege that is necessary to satisfy the information given. This will
	// often be called many times for a single query, as queries may have multiple points where information needs to be
	// validated.
	Handle(auth ast.AuthInformation) error
}
