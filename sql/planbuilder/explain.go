package planbuilder

import (
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"strings"
)

func (b *PlanBuilder) buildExplain(inScope *scope, n *sqlparser.Explain) (outScope *scope) {
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
