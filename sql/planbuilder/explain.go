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
