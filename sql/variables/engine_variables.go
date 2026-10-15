// Copyright 2026 Dolthub, Inc.
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

package variables

import "github.com/dolthub/go-mysql-server/sql"

func init() {
	sql.RegisterEngineVariablesFactory(NewEngineVariables)
}

// NewEngineVariables creates independent system and status registries for one
// engine. A bundle can be shared by sessions belonging to that engine.
func NewEngineVariables() *sql.EngineVariables {
	return &sql.EngineVariables{
		System: NewSystemVariableRegistry(),
		Status: NewStatusVariableRegistry(),
	}
}
