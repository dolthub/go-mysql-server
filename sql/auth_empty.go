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

package sql

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

// emptyAuthorizationHandlerFactory is the AuthorizationHandlerFactory for emptyAuthorizationHandler.
type emptyAuthorizationHandlerFactory struct{}

var _ AuthorizationHandlerFactory = emptyAuthorizationHandlerFactory{}

// CreateHandler implements the AuthorizationHandlerFactory interface.
func (emptyAuthorizationHandlerFactory) CreateHandler(cat Catalog) AuthorizationHandler {
	return emptyAuthorizationHandler{}
}

// emptyAuthorizationHandler will always return a "true" result.
type emptyAuthorizationHandler struct{}

var _ AuthorizationHandler = emptyAuthorizationHandler{}

// NewQueryState implements the AuthorizationHandler interface.
func (emptyAuthorizationHandler) NewQueryState(ctx *Context) AuthorizationQueryState {
	return nil
}

// HandleAuth implements the AuthorizationHandler interface.
func (emptyAuthorizationHandler) HandleAuth(ctx *Context, aqs AuthorizationQueryState, auth ast.AuthInformation) error {
	return nil
}

// HandleAuthNode implements the AuthorizationHandler interface.
func (emptyAuthorizationHandler) HandleAuthNode(ctx *Context, state AuthorizationQueryState, node AuthorizationCheckerNode) error {
	return nil
}

// CheckDatabase implements the AuthorizationHandler interface.
func (emptyAuthorizationHandler) CheckDatabase(ctx *Context, aqs AuthorizationQueryState, dbName string) error {
	return nil
}

// CheckSchema implements the AuthorizationHandler interface.
func (emptyAuthorizationHandler) CheckSchema(ctx *Context, aqs AuthorizationQueryState, dbName string, schemaName string) error {
	return nil
}

// CheckTable implements the AuthorizationHandler interface.
func (emptyAuthorizationHandler) CheckTable(ctx *Context, aqs AuthorizationQueryState, dbName string, schemaName string, tableName string) error {
	return nil
}
