package planbuilder

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/transform"
	"strings"
)

func buildProject(p *plan.Project) sql.Node {
	{
		// redundant projection
		if len(p.Child.Schema()) == len(p.Projections) {
			equal := true
			for i, c := range p.Child.Schema() {
				cmp := transform.ExpressionToColumn(p.Projections[i])
				if !strings.EqualFold(c.Name, cmp.Name) || !strings.EqualFold(c.Source, cmp.Source) {
					equal = false
					break
				}
			}
			if equal {
				return p.Child
			}
		}
	}
	return p
}
