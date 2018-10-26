package analyzer

import (
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

// CheckAuthorizationRule is a rule that validates that a user can execute
// the query.
const CheckAuthorizationRule = "check_authorization"

// CheckAuthorization creates an authorization check with the given Auth.
func CheckAuthorization(au auth.Auth) RuleFunc {
	return func(ctx *sql.Context, a *Analyzer, node sql.Node) (sql.Node, error) {
		var perm auth.Permission
		switch node.(type) {
		case *plan.InsertInto, *plan.DropIndex, *plan.CreateIndex, *plan.UnlockTables, *plan.LockTables:
			perm = auth.ReadPerm | auth.WritePerm
		default:
			perm = auth.ReadPerm
		}

		err := au.Allowed(ctx.User(), perm)
		if err != nil {
			return nil, err
		}

		return node, nil
	}
}
