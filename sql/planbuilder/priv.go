package planbuilder

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/plan"
)

func convertAccountName(names ...sqlparser.AccountName) []plan.UserName {
	userNames := make([]plan.UserName, len(names))
	for i, name := range names {
		userNames[i] = plan.UserName{
			Name:    name.Name,
			Host:    name.Host,
			AnyHost: name.AnyHost,
		}
	}
	return userNames
}

func convertPrivilege(privileges ...sqlparser.Privilege) []plan.Privilege {
	planPrivs := make([]plan.Privilege, len(privileges))
	for i, privilege := range privileges {
		var privType plan.PrivilegeType
		var dynamicString string
		switch privilege.Type {
		case sqlparser.PrivilegeType_All:
			privType = plan.PrivilegeType_All
		case sqlparser.PrivilegeType_Alter:
			privType = plan.PrivilegeType_Alter
		case sqlparser.PrivilegeType_AlterRoutine:
			privType = plan.PrivilegeType_AlterRoutine
		case sqlparser.PrivilegeType_Create:
			privType = plan.PrivilegeType_Create
		case sqlparser.PrivilegeType_CreateRole:
			privType = plan.PrivilegeType_CreateRole
		case sqlparser.PrivilegeType_CreateRoutine:
			privType = plan.PrivilegeType_CreateRoutine
		case sqlparser.PrivilegeType_CreateTablespace:
			privType = plan.PrivilegeType_CreateTablespace
		case sqlparser.PrivilegeType_CreateTemporaryTables:
			privType = plan.PrivilegeType_CreateTemporaryTables
		case sqlparser.PrivilegeType_CreateUser:
			privType = plan.PrivilegeType_CreateUser
		case sqlparser.PrivilegeType_CreateView:
			privType = plan.PrivilegeType_CreateView
		case sqlparser.PrivilegeType_Delete:
			privType = plan.PrivilegeType_Delete
		case sqlparser.PrivilegeType_Drop:
			privType = plan.PrivilegeType_Drop
		case sqlparser.PrivilegeType_DropRole:
			privType = plan.PrivilegeType_DropRole
		case sqlparser.PrivilegeType_Event:
			privType = plan.PrivilegeType_Event
		case sqlparser.PrivilegeType_Execute:
			privType = plan.PrivilegeType_Execute
		case sqlparser.PrivilegeType_File:
			privType = plan.PrivilegeType_File
		case sqlparser.PrivilegeType_GrantOption:
			privType = plan.PrivilegeType_GrantOption
		case sqlparser.PrivilegeType_Index:
			privType = plan.PrivilegeType_Index
		case sqlparser.PrivilegeType_Insert:
			privType = plan.PrivilegeType_Insert
		case sqlparser.PrivilegeType_LockTables:
			privType = plan.PrivilegeType_LockTables
		case sqlparser.PrivilegeType_Process:
			privType = plan.PrivilegeType_Process
		case sqlparser.PrivilegeType_References:
			privType = plan.PrivilegeType_References
		case sqlparser.PrivilegeType_Reload:
			privType = plan.PrivilegeType_Reload
		case sqlparser.PrivilegeType_ReplicationClient:
			privType = plan.PrivilegeType_ReplicationClient
		case sqlparser.PrivilegeType_ReplicationSlave:
			privType = plan.PrivilegeType_ReplicationSlave
		case sqlparser.PrivilegeType_Select:
			privType = plan.PrivilegeType_Select
		case sqlparser.PrivilegeType_ShowDatabases:
			privType = plan.PrivilegeType_ShowDatabases
		case sqlparser.PrivilegeType_ShowView:
			privType = plan.PrivilegeType_ShowView
		case sqlparser.PrivilegeType_Shutdown:
			privType = plan.PrivilegeType_Shutdown
		case sqlparser.PrivilegeType_Super:
			privType = plan.PrivilegeType_Super
		case sqlparser.PrivilegeType_Trigger:
			privType = plan.PrivilegeType_Trigger
		case sqlparser.PrivilegeType_Update:
			privType = plan.PrivilegeType_Update
		case sqlparser.PrivilegeType_Usage:
			privType = plan.PrivilegeType_Usage
		case sqlparser.PrivilegeType_Dynamic:
			privType = plan.PrivilegeType_Dynamic
			dynamicString = privilege.DynamicName
		default:
			// all privileges have been implemented, so if we hit the default something bad has happened
			panic("given privilege type parses but is not implemented")
		}
		planPrivs[i] = plan.Privilege{
			Type:    privType,
			Columns: privilege.Columns,
			Dynamic: dynamicString,
		}
	}
	return planPrivs
}

func convertObjectType(objType sqlparser.GrantObjectType) plan.ObjectType {
	switch objType {
	case sqlparser.GrantObjectType_Any:
		return plan.ObjectType_Any
	case sqlparser.GrantObjectType_Table:
		return plan.ObjectType_Table
	case sqlparser.GrantObjectType_Function:
		return plan.ObjectType_Function
	case sqlparser.GrantObjectType_Procedure:
		return plan.ObjectType_Procedure
	default:
		panic("no other grant object types exist")
	}
}

func convertPrivilegeLevel(privLevel sqlparser.PrivilegeLevel) plan.PrivilegeLevel {
	return plan.PrivilegeLevel{
		Database:     privLevel.Database,
		TableRoutine: privLevel.TableRoutine,
	}
}

func (b *PlanBuilder) buildCreateUser(inScope *scope, n *sqlparser.CreateUser) (*plan.CreateUser, error) {
	authUsers := make([]plan.AuthenticatedUser, len(n.Users))
	for i, user := range n.Users {
		authUser := plan.AuthenticatedUser{
			UserName: convertAccountName(user.AccountName)[0],
		}
		if user.Auth1 != nil {
			authUser.Identity = user.Auth1.Identity
			if user.Auth1.Plugin == "mysql_native_password" && len(user.Auth1.Password) > 0 {
				authUser.Auth1 = plan.AuthenticationMysqlNativePassword(user.Auth1.Password)
			} else if len(user.Auth1.Plugin) > 0 {
				authUser.Auth1 = plan.NewOtherAuthentication(user.Auth1.Password, user.Auth1.Plugin)
			} else {
				// We default to using the password, even if it's empty
				authUser.Auth1 = plan.NewDefaultAuthentication(user.Auth1.Password)
			}
		}
		if user.Auth2 != nil || user.Auth3 != nil || user.AuthInitial != nil {
			return nil, fmt.Errorf(`multi-factor authentication is not yet supported`)
		}
		//TODO: figure out how to represent the remaining authentication methods and multi-factor auth
		authUsers[i] = authUser
	}
	var tlsOptions *plan.TLSOptions
	if n.TLSOptions != nil {
		tlsOptions = &plan.TLSOptions{
			SSL:     n.TLSOptions.SSL,
			X509:    n.TLSOptions.X509,
			Cipher:  n.TLSOptions.Cipher,
			Issuer:  n.TLSOptions.Issuer,
			Subject: n.TLSOptions.Subject,
		}
	}
	var accountLimits *plan.AccountLimits
	if n.AccountLimits != nil {
		var maxQueries *int64
		if n.AccountLimits.MaxQueriesPerHour != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxQueriesPerHour.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxQueries = &val
			}
		}
		var maxUpdates *int64
		if n.AccountLimits.MaxUpdatesPerHour != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxUpdatesPerHour.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxUpdates = &val
			}
		}
		var maxConnections *int64
		if n.AccountLimits.MaxConnectionsPerHour != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxConnectionsPerHour.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxConnections = &val
			}
		}
		var maxUserConnections *int64
		if n.AccountLimits.MaxUserConnections != nil {
			if val, err := strconv.ParseInt(string(n.AccountLimits.MaxUserConnections.Val), 10, 64); err != nil {
				return nil, err
			} else {
				maxUserConnections = &val
			}
		}
		accountLimits = &plan.AccountLimits{
			MaxQueriesPerHour:     maxQueries,
			MaxUpdatesPerHour:     maxUpdates,
			MaxConnectionsPerHour: maxConnections,
			MaxUserConnections:    maxUserConnections,
		}
	}
	var passwordOptions *plan.PasswordOptions
	if n.PasswordOptions != nil {
		var expirationTime *int64
		if n.PasswordOptions.ExpirationTime != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.ExpirationTime.Val), 10, 64); err != nil {
				return nil, err
			} else {
				expirationTime = &val
			}
		}
		var history *int64
		if n.PasswordOptions.History != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.History.Val), 10, 64); err != nil {
				return nil, err
			} else {
				history = &val
			}
		}
		var reuseInterval *int64
		if n.PasswordOptions.ReuseInterval != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.ReuseInterval.Val), 10, 64); err != nil {
				return nil, err
			} else {
				reuseInterval = &val
			}
		}
		var failedAttempts *int64
		if n.PasswordOptions.FailedAttempts != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.FailedAttempts.Val), 10, 64); err != nil {
				return nil, err
			} else {
				failedAttempts = &val
			}
		}
		var lockTime *int64
		if n.PasswordOptions.LockTime != nil {
			if val, err := strconv.ParseInt(string(n.PasswordOptions.LockTime.Val), 10, 64); err != nil {
				return nil, err
			} else {
				lockTime = &val
			}
		}
		passwordOptions = &plan.PasswordOptions{
			RequireCurrentOptional: n.PasswordOptions.RequireCurrentOptional,
			ExpirationTime:         expirationTime,
			History:                history,
			ReuseInterval:          reuseInterval,
			FailedAttempts:         failedAttempts,
			LockTime:               lockTime,
		}
	}
	return &plan.CreateUser{
		IfNotExists:     n.IfNotExists,
		Users:           authUsers,
		DefaultRoles:    convertAccountName(n.DefaultRoles...),
		TLSOptions:      tlsOptions,
		AccountLimits:   accountLimits,
		PasswordOptions: passwordOptions,
		Locked:          n.Locked,
		Attribute:       n.Attribute,
		MySQLDb:         sql.UnresolvedDatabase("mysql"),
	}, nil
}

func (b *PlanBuilder) buildRenameUser(inScope *scope, n *sqlparser.RenameUser) (*plan.RenameUser, error) {
	oldNames := make([]plan.UserName, len(n.Accounts))
	newNames := make([]plan.UserName, len(n.Accounts))
	for i, account := range n.Accounts {
		oldNames[i] = convertAccountName(account.From)[0]
		newNames[i] = convertAccountName(account.To)[0]
	}
	return plan.NewRenameUser(oldNames, newNames), nil
}

func (b *PlanBuilder) buildGrantPrivilege(inScope *scope, n *sqlparser.GrantPrivilege) (*plan.Grant, error) {
	var gau *plan.GrantUserAssumption
	if n.As != nil {
		gauType := plan.GrantUserAssumptionType_Default
		switch n.As.Type {
		case sqlparser.GrantUserAssumptionType_None:
			gauType = plan.GrantUserAssumptionType_None
		case sqlparser.GrantUserAssumptionType_All:
			gauType = plan.GrantUserAssumptionType_All
		case sqlparser.GrantUserAssumptionType_AllExcept:
			gauType = plan.GrantUserAssumptionType_AllExcept
		case sqlparser.GrantUserAssumptionType_Roles:
			gauType = plan.GrantUserAssumptionType_Roles
		}
		gau = &plan.GrantUserAssumption{
			Type:  gauType,
			User:  convertAccountName(n.As.User)[0],
			Roles: convertAccountName(n.As.Roles...),
		}
	}
	return plan.NewGrant(
		sql.UnresolvedDatabase("mysql"),
		convertPrivilege(n.Privileges...),
		convertObjectType(n.ObjectType),
		convertPrivilegeLevel(n.PrivilegeLevel),
		convertAccountName(n.To...),
		n.WithGrantOption,
		gau,
		b.ctx.Session.Client().User,
	)
}

func (b *PlanBuilder) buildShowGrants(inScope *scope, n *sqlparser.ShowGrants) (*plan.ShowGrants, error) {
	var currentUser bool
	var user *plan.UserName
	if n.For != nil {
		currentUser = false
		user = &convertAccountName(*n.For)[0]
	} else {
		currentUser = true
		client := b.ctx.Session.Client()
		user = &plan.UserName{
			Name:    client.User,
			Host:    client.Address,
			AnyHost: client.Address == "%",
		}
	}
	return plan.NewShowGrants(currentUser, user, convertAccountName(n.Using...)), nil
}

func (b *PlanBuilder) buildFlush(inScope *scope, f *sqlparser.Flush) (sql.Node, error) {
	var writesToBinlog = true
	switch strings.ToLower(f.Type) {
	case "no_write_to_binlog", "local":
		//writesToBinlog = false
		return nil, fmt.Errorf("%s not supported", f.Type)
	}

	switch strings.ToLower(f.Option.Name) {
	case "privileges":
		return plan.NewFlushPrivileges(writesToBinlog), nil
	default:
		return nil, fmt.Errorf("%s not supported", f.Option.Name)
	}
}
