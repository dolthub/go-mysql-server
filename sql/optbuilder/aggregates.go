package optbuilder

import ast "github.com/dolthub/vitess/go/vt/sqlparser"

var _ ast.Expr = (*aggregateInfo)(nil)

type aggregateInfo struct {
	ast.Expr
}
