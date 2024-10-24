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

import ast "github.com/dolthub/vitess/go/vt/sqlparser"

// errorQueryAuthorizationHandler is used to cache errors encountered during the creation of a QueryAuthorizationHandler.
// To preserve the transactional flow, the error will be returned during the Handle call, rather than immediately.
type errorQueryAuthorizationHandler struct {
	err error
}

var _ QueryAuthorizationHandler = errorQueryAuthorizationHandler{}

// Handle implements the QueryAuthorizationHandler interface.
func (h errorQueryAuthorizationHandler) Handle(auth ast.AuthInformation) error {
	return h.err
}
