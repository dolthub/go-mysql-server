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

import (
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql/plan"
)

func (b *Builder) buildExplain(inScope *scope, n *sqlparser.Explain) (outScope *scope) {
	outScope = inScope.push()
	childScope := b.build(inScope, n.Statement, "")
	explainFmt := sqlparser.TreeStr
	switch strings.ToLower(n.ExplainFormat) {
	case "", sqlparser.TreeStr:
	// tree format, do nothing
	case "debug":
		explainFmt = "debug"
	default:
		err := errInvalidDescribeFormat.New(n.ExplainFormat, "tree")
		b.handleErr(err)
	}

	outScope.node = plan.NewDescribeQuery(explainFmt, childScope.node)
	return outScope
}
