// Copyright 2022 Dolthub, Inc.
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

package mysql_db

import (
	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db/serial"
)

// serializePrivilegeTypes writes the given PrivilegeTypes into the flatbuffer Builder using the given flatbuffer start function, and returns the offset
// This helper function is used by PrivilegeSetColumn, PrivilegeSetTable, and PrivilegeSetDatabase
func serializePrivilegeTypes(b *flatbuffers.Builder, StartPTVector func(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT, pts []sql.PrivilegeType) flatbuffers.UOffsetT {
	// Order doesn't matter since it's a set of indexes
	StartPTVector(b, len(pts))
	for _, gs := range pts {
		b.PrependInt32(int32(gs))
	}
	return b.EndVector(len(pts))
}

// TODO: should have a generic serialize strings helper method if used in future

// serializeVectorOffsets writes the given offsets slice to the flatbuffer Builder using the given start vector function, and returns the offset
func serializeVectorOffsets(b *flatbuffers.Builder, StartVector func(builder *flatbuffers.Builder, numElems int) flatbuffers.UOffsetT, offsets []flatbuffers.UOffsetT) flatbuffers.UOffsetT {
	// Expect the given offsets slice to already be in reverse order
	StartVector(b, len(offsets))
	for _, offset := range offsets {
		b.PrependUOffsetT(offset)
	}
	return b.EndVector(len(offsets))
}

// serializeStrings writes the set of strings to flatbuffer builder, and returns offset of resulting vector
func serializeGlobalDynamic(b *flatbuffers.Builder, strs map[string]struct{}) flatbuffers.UOffsetT {
	// Write strings, and save offsets
	i := 0
	offsets := make([]flatbuffers.UOffsetT, len(strs))
	for str := range strs {
		offsets[i] = b.CreateString(str)
		i++
	}
	// Write string offsets (already reversed)
	return serializeVectorOffsets(b, serial.PrivilegeSetStartGlobalDynamicVector, offsets)
}

func serializeColumns(b *flatbuffers.Builder, columns []PrivilegeSetColumn) flatbuffers.UOffsetT {
	// Write column variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(columns))
	for i, column := range columns {
		name := b.CreateString(column.Name())
		privs := serializePrivilegeTypes(b, serial.PrivilegeSetColumnStartPrivsVector, column.ToSlice())

		serial.PrivilegeSetColumnStart(b)
		serial.PrivilegeSetColumnAddName(b, name)
		serial.PrivilegeSetColumnAddPrivs(b, privs)
		offsets[len(offsets)-i-1] = serial.PrivilegeSetColumnEnd(b) // reverse order
	}
	// Write column offsets (already reversed)
	return serializeVectorOffsets(b, serial.PrivilegeSetTableStartColumnsVector, offsets)
}

func serializeTables(b *flatbuffers.Builder, tables []PrivilegeSetTable) flatbuffers.UOffsetT {
	// Write table variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(tables))
	for i, table := range tables {
		name := b.CreateString(table.Name())
		privs := serializePrivilegeTypes(b, serial.PrivilegeSetTableStartPrivsVector, table.ToSlice())
		cols := serializeColumns(b, table.getColumns())

		serial.PrivilegeSetTableStart(b)
		serial.PrivilegeSetTableAddName(b, name)
		serial.PrivilegeSetTableAddPrivs(b, privs)
		serial.PrivilegeSetTableAddColumns(b, cols)
		offsets[len(offsets)-i-1] = serial.PrivilegeSetTableEnd(b) // reverse order
	}
	// Write table offsets (order already reversed)
	return serializeVectorOffsets(b, serial.PrivilegeSetDatabaseStartTablesVector, offsets)
}

// serializeDatabases writes the given Privilege Set Databases into the flatbuffer Builder, and returns the offset
func serializeDatabases(b *flatbuffers.Builder, databases []PrivilegeSetDatabase) flatbuffers.UOffsetT {
	// Write database variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(databases))
	for i, database := range databases {
		name := b.CreateString(database.Name())
		privs := serializePrivilegeTypes(b, serial.PrivilegeSetDatabaseStartPrivsVector, database.ToSlice())
		tables := serializeTables(b, database.getTables())

		serial.PrivilegeSetDatabaseStart(b)
		serial.PrivilegeSetDatabaseAddName(b, name)
		serial.PrivilegeSetDatabaseAddPrivs(b, privs)
		serial.PrivilegeSetDatabaseAddTables(b, tables)
		offsets[len(offsets)-i-1] = serial.PrivilegeSetDatabaseEnd(b)
	}

	// Write database offsets (order already reversed)
	return serializeVectorOffsets(b, serial.PrivilegeSetStartDatabasesVector, offsets)
}

func serializePrivilegeSet(b *flatbuffers.Builder, ps *PrivilegeSet) flatbuffers.UOffsetT {
	// Write privilege set variables, and save offsets
	globalStatic := serializePrivilegeTypes(b, serial.PrivilegeSetStartGlobalStaticVector, ps.ToSlice())
	globalDynamic := serializeGlobalDynamic(b, ps.globalDynamic)
	databases := serializeDatabases(b, ps.getDatabases())

	// Write PrivilegeSet
	serial.PrivilegeSetStart(b)
	serial.PrivilegeSetAddGlobalStatic(b, globalStatic)
	serial.PrivilegeSetAddGlobalDynamic(b, globalDynamic)
	serial.PrivilegeSetAddDatabases(b, databases)
	return serial.PrivilegeSetEnd(b)
}

// serializeAttributes will deference and write the given string pointer to the flatbuffer builder and will return the
// offset. Will return 0 for offset if string pointer is null; this causes the accessor to also return nil when loading
func serializeAttributes(b *flatbuffers.Builder, attributes *string) flatbuffers.UOffsetT {
	if attributes == nil {
		return 0
	} else {
		return b.CreateString(*attributes)
	}
}

func serializeUser(b *flatbuffers.Builder, users []*User) flatbuffers.UOffsetT {
	// Write user variables, and save offsets
	offsets := make([]flatbuffers.UOffsetT, len(users))
	for i, user := range users {
		userName := b.CreateString(user.User)
		host := b.CreateString(user.Host)
		privilegeSet := serializePrivilegeSet(b, &user.PrivilegeSet)
		plugin := b.CreateString(user.Plugin)
		password := b.CreateString(user.Password)
		attributes := serializeAttributes(b, user.Attributes)
		identity := b.CreateString(user.Identity)

		serial.UserStart(b)
		serial.UserAddUser(b, userName)
		serial.UserAddHost(b, host)
		serial.UserAddPrivilegeSet(b, privilegeSet)
		serial.UserAddPlugin(b, plugin)
		serial.UserAddPassword(b, password)
		serial.UserAddPasswordLastChanged(b, user.PasswordLastChanged.Unix())
		serial.UserAddLocked(b, user.Locked)
		serial.UserAddAttributes(b, attributes)
		serial.UserAddIdentity(b, identity)

		offsets[len(users)-i-1] = serial.UserEnd(b) // reverse order
	}

	// Write user offsets (already in reverse order)
	return serializeVectorOffsets(b, serial.MySQLDbStartUserVector, offsets)
}

func serializeRoleEdge(b *flatbuffers.Builder, roleEdges []*RoleEdge) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(roleEdges))
	for i, roleEdge := range roleEdges {
		// Serialize each of the member vars in RoleEdge and save their offsets
		fromHost := b.CreateString(roleEdge.FromHost)
		fromUser := b.CreateString(roleEdge.FromUser)
		toHost := b.CreateString(roleEdge.ToHost)
		toUser := b.CreateString(roleEdge.ToUser)

		// Start RoleEdge
		serial.RoleEdgeStart(b)

		// Write their offsets to flatbuffer builder
		serial.RoleEdgeAddFromHost(b, fromHost)
		serial.RoleEdgeAddFromUser(b, fromUser)
		serial.RoleEdgeAddToHost(b, toHost)
		serial.RoleEdgeAddToUser(b, toUser)

		// Write WithAdminOption (boolean value doesn't need offset)
		serial.RoleEdgeAddWithAdminOption(b, roleEdge.WithAdminOption)

		// End RoleEdge
		offsets[len(roleEdges)-i-1] = serial.RoleEdgeEnd(b) // reverse order
	}

	// Write role_edges vector (already in reversed order)
	return serializeVectorOffsets(b, serial.MySQLDbStartRoleEdgesVector, offsets)
}

func serializeReplicaSourceInfo(b *flatbuffers.Builder, replicaSourceInfos []*ReplicaSourceInfo) flatbuffers.UOffsetT {
	offsets := make([]flatbuffers.UOffsetT, len(replicaSourceInfos))

	for i, replicaSourceInfo := range replicaSourceInfos {
		host := b.CreateString(replicaSourceInfo.Host)
		user := b.CreateString(replicaSourceInfo.User)
		password := b.CreateString(replicaSourceInfo.Password)
		uuid := b.CreateString(replicaSourceInfo.Uuid)

		// Start ReplicaSourceInfo
		serial.ReplicaSourceInfoStart(b)

		// Write their offsets to flatbuffer builder
		serial.ReplicaSourceInfoAddHost(b, host)
		serial.ReplicaSourceInfoAddUser(b, user)
		serial.ReplicaSourceInfoAddPassword(b, password)
		serial.ReplicaSourceInfoAddUuid(b, uuid)

		// Write Port (uint value doesn't need offset)
		serial.ReplicaSourceInfoAddPort(b, replicaSourceInfo.Port)

		// End ReplicaSourceInfo
		offsets[len(replicaSourceInfos)-i-1] = serial.ReplicaSourceInfoEnd(b)
	}

	// Write replica source info vector (already in reversed order)
	return serializeVectorOffsets(b, serial.MySQLDbStartReplicaSourceInfoVector, offsets)
}
