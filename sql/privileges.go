package sql

// PrivilegedOperation represents an operation that requires privileges to execute.
type PrivilegedOperation struct {
	Database   string
	Table      string
	Column     string
	Privileges []PrivilegeType
}

// NewPrivilegedOperation returns a new PrivilegedOperation with the given parameters.
func NewPrivilegedOperation(dbName string, tblName string, colName string, privs ...PrivilegeType) PrivilegedOperation {
	return PrivilegedOperation{
		Database:   dbName,
		Table:      tblName,
		Column:     colName,
		Privileges: privs,
	}
}

// PrivilegedOperationChecker contains the necessary data to check whether the operation should succeed based on the
// privileges contained by the user. The user is retrieved from the context, along with their active roles.
type PrivilegedOperationChecker interface {
	// UserHasPrivileges fetches the User, and returns whether they have the desired privileges necessary to perform the
	// privileged operation(s). This takes into account the active roles, which are set in the context, therefore both
	// the user and the active roles are pulled from the context.
	UserHasPrivileges(ctx *Context, operations ...PrivilegedOperation) bool
}

// PrivilegeType represents a privilege.
type PrivilegeType int

const (
	PrivilegeType_Select PrivilegeType = iota
	PrivilegeType_Insert
	PrivilegeType_Update
	PrivilegeType_Delete
	PrivilegeType_Create
	PrivilegeType_Drop
	PrivilegeType_Reload
	PrivilegeType_Shutdown
	PrivilegeType_Process
	PrivilegeType_File
	PrivilegeType_Grant
	PrivilegeType_References
	PrivilegeType_Index
	PrivilegeType_Alter
	PrivilegeType_ShowDB
	PrivilegeType_Super
	PrivilegeType_CreateTempTable
	PrivilegeType_LockTables
	PrivilegeType_Execute
	PrivilegeType_ReplicationSlave
	PrivilegeType_ReplicationClient
	PrivilegeType_CreateView
	PrivilegeType_ShowView
	PrivilegeType_CreateRoutine
	PrivilegeType_AlterRoutine
	PrivilegeType_CreateUser
	PrivilegeType_Event
	PrivilegeType_Trigger
	PrivilegeType_CreateTablespace
	PrivilegeType_CreateRole
	PrivilegeType_DropRole
)

// privilegeTypeStrings are in the same order as the enumerations above, so that it's a simple index access.
var privilegeTypeStrings = []string{
	"SELECT",
	"INSERT",
	"UPDATE",
	"DELETE",
	"CREATE",
	"DROP",
	"RELOAD",
	"SHUTDOWN",
	"PROCESS",
	"FILE",
	"GRANT",
	"REFERENCES",
	"INDEX",
	"ALTER",
	"SHOW DATABASES",
	"SUPER",
	"CREATE TEMPORARY TABLES",
	"LOCK TABLES",
	"EXECUTE",
	"REPLICATION SLAVE",
	"REPLICATION CLIENT",
	"CREATE VIEW",
	"SHOW VIEW",
	"CREATE ROUTINE",
	"ALTER ROUTINE",
	"CREATE USER",
	"EVENT",
	"TRIGGER",
	"CREATE TABLESPACE",
	"CREATE ROLE",
	"DROP ROLE",
}

// String returns the sql.PrivilegeType as a string, for display in places such as "SHOW GRANTS".
func (pt PrivilegeType) String() string {
	return privilegeTypeStrings[pt]
}

// privilegeTypeStringMap map each string (same ones in privilegeTypeStrings) to their appropriate PrivilegeType.
var privilegeTypeStringMap = map[string]PrivilegeType{
	"SELECT":                  PrivilegeType_Select,
	"INSERT":                  PrivilegeType_Insert,
	"UPDATE":                  PrivilegeType_Update,
	"DELETE":                  PrivilegeType_Delete,
	"CREATE":                  PrivilegeType_Create,
	"DROP":                    PrivilegeType_Drop,
	"RELOAD":                  PrivilegeType_Reload,
	"SHUTDOWN":                PrivilegeType_Shutdown,
	"PROCESS":                 PrivilegeType_Process,
	"FILE":                    PrivilegeType_File,
	"GRANT":                   PrivilegeType_Grant,
	"REFERENCES":              PrivilegeType_References,
	"INDEX":                   PrivilegeType_Index,
	"ALTER":                   PrivilegeType_Alter,
	"SHOW DATABASES":          PrivilegeType_ShowDB,
	"SUPER":                   PrivilegeType_Super,
	"CREATE TEMPORARY TABLES": PrivilegeType_CreateTempTable,
	"LOCK TABLES":             PrivilegeType_LockTables,
	"EXECUTE":                 PrivilegeType_Execute,
	"REPLICATION SLAVE":       PrivilegeType_ReplicationSlave,
	"REPLICATION CLIENT":      PrivilegeType_ReplicationClient,
	"CREATE VIEW":             PrivilegeType_CreateView,
	"SHOW VIEW":               PrivilegeType_ShowView,
	"CREATE ROUTINE":          PrivilegeType_CreateRoutine,
	"ALTER ROUTINE":           PrivilegeType_AlterRoutine,
	"CREATE USER":             PrivilegeType_CreateUser,
	"EVENT":                   PrivilegeType_Event,
	"TRIGGER":                 PrivilegeType_Trigger,
	"CREATE TABLESPACE":       PrivilegeType_CreateTablespace,
	"CREATE ROLE":             PrivilegeType_CreateRole,
	"DROP ROLE":               PrivilegeType_DropRole,
}

// PrivilegeTypeFromString returns the matching PrivilegeType for the given string. If there is no match, returns false.
func PrivilegeTypeFromString(privilegeType string) (PrivilegeType, bool) {
	match, ok := privilegeTypeStringMap[privilegeType]
	return match, ok
}
