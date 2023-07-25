package planbuilder

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql/plan"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"strconv"
	"unicode"
)

func (b *Builder) buildCreateSpatialRefSys(inScope *scope, n *ast.CreateSpatialRefSys) (outScope *scope) {
	outScope = inScope.push()
	srid, err := strconv.ParseInt(string(n.SRID.Val), 10, 16)
	if err != nil {
		b.handleErr(err)
	}

	if n.SrsAttr == nil {
		b.handleErr(fmt.Errorf("missing attribute"))
	}

	if n.SrsAttr.Name == "" {
		b.handleErr(fmt.Errorf("missing mandatory attribute NAME"))
	}
	if unicode.IsSpace(rune(n.SrsAttr.Name[0])) || unicode.IsSpace(rune(n.SrsAttr.Name[len(n.SrsAttr.Name)-1])) {
		b.handleErr(fmt.Errorf("the spatial reference system name can't be an empty string or start or end with whitespace"))
	}
	// TODO: there are additional rules to validate the attribute definition
	if n.SrsAttr.Definition == "" {
		b.handleErr(fmt.Errorf("missing mandatory attribute DEFINITION"))
	}
	if n.SrsAttr.Organization == "" {
		b.handleErr(fmt.Errorf("missing mandatory attribute ORGANIZATION NAME"))
	}
	if unicode.IsSpace(rune(n.SrsAttr.Organization[0])) || unicode.IsSpace(rune(n.SrsAttr.Organization[len(n.SrsAttr.Organization)-1])) {
		b.handleErr(fmt.Errorf("the organization name can't be an empty string or start or end with whitespace"))
	}
	if n.SrsAttr.OrgID == nil {
		b.handleErr(fmt.Errorf("missing mandatory attribute ORGANIZATION ID"))
	}
	orgID, err := strconv.ParseInt(string(n.SrsAttr.OrgID.Val), 10, 16)
	if err != nil {
		b.handleErr(err)
	}

	srsAttr := plan.SrsAttribute{
		Name:         n.SrsAttr.Name,
		Definition:   n.SrsAttr.Definition,
		Organization: n.SrsAttr.Organization,
		OrgID:        uint32(orgID),
		Description:  n.SrsAttr.Description,
	}
	newN, err := plan.NewCreateSpatialRefSys(uint32(srid), n.OrReplace, n.IfNotExists, srsAttr)
	if err != nil {
		b.handleErr(err)
	}
	outScope.node = newN
	return outScope
}
