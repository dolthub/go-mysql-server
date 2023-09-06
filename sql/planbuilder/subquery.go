// Copyright 2023 Dolthub, Inc.
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

import "github.com/dolthub/go-mysql-server/sql"

type subquery struct {
	correlated sql.ColSet
	volatile   bool
}

func (s *subquery) addOutOfScope(c columnId) {
	s.correlated.Add(sql.ColumnId(c))
}

func (s *subquery) markVolatile() {
	s.volatile = true
}
