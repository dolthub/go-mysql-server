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

package grant_tables

import (
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
)

// PrivilegeSet is a set containing privileges. Due to the nested sets potentially returning empty sets, this also acts
// as the singular location to modify all nested sets.
type PrivilegeSet struct {
	globalStatic  map[sql.PrivilegeType]struct{}
	globalDynamic map[string]struct{}
	databases     map[string]PrivilegeSetDatabase
}

// NewPrivilegeSet returns a new PrivilegeSet.
func NewPrivilegeSet() PrivilegeSet {
	return PrivilegeSet{
		make(map[sql.PrivilegeType]struct{}),
		make(map[string]struct{}),
		make(map[string]PrivilegeSetDatabase),
	}
}

// newPrivilegeSetWithAllPrivileges returns a new PrivilegeSet with every global static privilege added.
func newPrivilegeSetWithAllPrivileges() PrivilegeSet {
	return PrivilegeSet{
		map[sql.PrivilegeType]struct{}{
			sql.PrivilegeType_Select:            {},
			sql.PrivilegeType_Insert:            {},
			sql.PrivilegeType_Update:            {},
			sql.PrivilegeType_Delete:            {},
			sql.PrivilegeType_Create:            {},
			sql.PrivilegeType_Drop:              {},
			sql.PrivilegeType_Reload:            {},
			sql.PrivilegeType_Shutdown:          {},
			sql.PrivilegeType_Process:           {},
			sql.PrivilegeType_File:              {},
			sql.PrivilegeType_Grant:             {},
			sql.PrivilegeType_References:        {},
			sql.PrivilegeType_Index:             {},
			sql.PrivilegeType_Alter:             {},
			sql.PrivilegeType_ShowDB:            {},
			sql.PrivilegeType_Super:             {},
			sql.PrivilegeType_CreateTempTable:   {},
			sql.PrivilegeType_LockTables:        {},
			sql.PrivilegeType_Execute:           {},
			sql.PrivilegeType_ReplicationSlave:  {},
			sql.PrivilegeType_ReplicationClient: {},
			sql.PrivilegeType_CreateView:        {},
			sql.PrivilegeType_ShowView:          {},
			sql.PrivilegeType_CreateRoutine:     {},
			sql.PrivilegeType_AlterRoutine:      {},
			sql.PrivilegeType_CreateUser:        {},
			sql.PrivilegeType_Event:             {},
			sql.PrivilegeType_Trigger:           {},
			sql.PrivilegeType_CreateTablespace:  {},
			sql.PrivilegeType_CreateRole:        {},
			sql.PrivilegeType_DropRole:          {},
		},
		make(map[string]struct{}),
		make(map[string]PrivilegeSetDatabase),
	}
}

// AddGlobalStatic adds the given global static privilege(s).
func (ps PrivilegeSet) AddGlobalStatic(privileges ...sql.PrivilegeType) {
	for _, priv := range privileges {
		ps.globalStatic[priv] = struct{}{}
	}
}

// AddGlobalDynamic adds the given global dynamic privilege(s).
func (ps PrivilegeSet) AddGlobalDynamic(privileges ...string) {
	for _, priv := range privileges {
		ps.globalDynamic[priv] = struct{}{}
	}
}

// AddDatabase adds the given database privilege(s).
func (ps PrivilegeSet) AddDatabase(dbName string, privileges ...sql.PrivilegeType) {
	dbSet := ps.getUseableDb(dbName)
	for _, priv := range privileges {
		dbSet.privs[priv] = struct{}{}
	}
}

// AddTable adds the given table privilege(s).
func (ps PrivilegeSet) AddTable(dbName string, tblName string, privileges ...sql.PrivilegeType) {
	tblSet := ps.getUseableDb(dbName).getUseableTbl(tblName)
	for _, priv := range privileges {
		tblSet.privs[priv] = struct{}{}
	}
}

// AddColumn adds the given column privilege(s).
func (ps PrivilegeSet) AddColumn(dbName string, tblName string, colName string, privileges ...sql.PrivilegeType) {
	colSet := ps.getUseableDb(dbName).getUseableTbl(tblName).getUseableCol(colName)
	for _, priv := range privileges {
		colSet.privs[priv] = struct{}{}
	}
}

// RemoveGlobalStatic removes the given global static privilege(s).
func (ps PrivilegeSet) RemoveGlobalStatic(privileges ...sql.PrivilegeType) {
	for _, priv := range privileges {
		delete(ps.globalStatic, priv)
	}
}

// RemoveGlobalDynamic removes the given global dynamic privilege(s).
func (ps PrivilegeSet) RemoveGlobalDynamic(privileges ...string) {
	for _, priv := range privileges {
		delete(ps.globalDynamic, priv)
	}
}

// RemoveDatabase removes the given database privilege(s).
func (ps PrivilegeSet) RemoveDatabase(dbName string, privileges ...sql.PrivilegeType) {
	// We don't use the getUseableDb function since we don't want to create a new map if it doesn't already exist
	dbSet := ps.Database(dbName)
	if len(dbSet.privs) > 0 {
		for _, priv := range privileges {
			delete(dbSet.privs, priv)
		}
	}
}

// RemoveTable removes the given table privilege(s).
func (ps PrivilegeSet) RemoveTable(dbName string, tblName string, privileges ...sql.PrivilegeType) {
	// We don't use the getUseable functions since we don't want to create new maps if they don't already exist
	tblSet := ps.Database(dbName).Table(tblName)
	if len(tblSet.privs) > 0 {
		for _, priv := range privileges {
			delete(tblSet.privs, priv)
		}
	}
}

// RemoveColumn removes the given column privilege(s).
func (ps PrivilegeSet) RemoveColumn(dbName string, tblName string, colName string, privileges ...sql.PrivilegeType) {
	// We don't use the getUseable functions since we don't want to create new maps if they don't already exist
	colSet := ps.Database(dbName).Table(tblName).Column(colName)
	if len(colSet.privs) > 0 {
		for _, priv := range privileges {
			delete(colSet.privs, priv)
		}
	}
}

// Has returns whether the given global static privilege(s) exists.
func (ps PrivilegeSet) Has(privileges ...sql.PrivilegeType) bool {
	for _, priv := range privileges {
		if _, ok := ps.globalStatic[priv]; !ok {
			return false
		}
	}
	return true
}

// HasPrivileges returns whether this PrivilegeSet has any privileges at any level.
func (ps PrivilegeSet) HasPrivileges() bool {
	if len(ps.globalStatic) > 0 || len(ps.globalDynamic) > 0 {
		return true
	}
	for _, dbSet := range ps.databases {
		if dbSet.HasPrivileges() {
			return true
		}
	}
	return false
}

// GlobalCount returns the combined number of global static and global dynamic privileges.
func (ps PrivilegeSet) GlobalCount() int {
	return len(ps.globalStatic) + len(ps.globalDynamic)
}

// StaticCount returns the number of global static privileges, while not including global dynamic privileges.
func (ps PrivilegeSet) StaticCount() int {
	return len(ps.globalStatic)
}

// Database returns the set of privileges for the given database. Returns an empty set if the database does not exist.
func (ps PrivilegeSet) Database(dbName string) PrivilegeSetDatabase {
	dbSet, ok := ps.databases[strings.ToLower(dbName)]
	if ok {
		return dbSet
	}
	return PrivilegeSetDatabase{name: dbName}
}

// GetDatabases returns all databases.
func (ps PrivilegeSet) GetDatabases() []PrivilegeSetDatabase {
	dbSets := make([]PrivilegeSetDatabase, len(ps.databases))
	i := 0
	for _, dbSet := range ps.databases {
		// Only return databases that have a database-level privilege, or a privilege on an underlying table or column.
		// Otherwise, there is no difference between the returned database and the zero-value for any database.
		if dbSet.HasPrivileges() {
			dbSets[i] = dbSet
			i++
		}
	}
	return dbSets
}

// UnionWith merges the given set of privileges to the calling set of privileges.
func (ps PrivilegeSet) UnionWith(other PrivilegeSet) {
	for priv := range other.globalStatic {
		ps.globalStatic[priv] = struct{}{}
	}
	for priv := range other.globalDynamic {
		ps.globalDynamic[priv] = struct{}{}
	}
	for _, otherDbSet := range other.databases {
		ps.getUseableDb(otherDbSet.name).unionWith(otherDbSet)
	}
}

// ClearGlobal removes all global privileges.
func (ps *PrivilegeSet) ClearGlobal() {
	ps.globalStatic = make(map[sql.PrivilegeType]struct{})
	ps.globalDynamic = make(map[string]struct{})
}

// ClearDatabase removes all privileges for the given database.
func (ps PrivilegeSet) ClearDatabase(dbName string) {
	ps.getUseableDb(dbName).clear()
}

// ClearTable removes all privileges for the given table.
func (ps PrivilegeSet) ClearTable(dbName string, tblName string) {
	ps.getUseableDb(dbName).getUseableTbl(tblName).clear()
}

// ClearColumn removes all privileges for the given column.
func (ps PrivilegeSet) ClearColumn(dbName string, tblName string, colName string) {
	ps.getUseableDb(dbName).getUseableTbl(tblName).getUseableCol(colName).clear()
}

// ClearAll removes all privileges.
func (ps *PrivilegeSet) ClearAll() {
	ps.globalStatic = make(map[sql.PrivilegeType]struct{})
	ps.globalDynamic = make(map[string]struct{})
	ps.databases = make(map[string]PrivilegeSetDatabase)
}

// Equals returns whether the given set of privileges is equivalent to the calling set.
func (ps PrivilegeSet) Equals(otherPs PrivilegeSet) bool {
	if len(ps.globalStatic) != len(otherPs.globalStatic) ||
		len(ps.globalDynamic) != len(otherPs.globalDynamic) ||
		len(ps.databases) != len(otherPs.databases) {
		return false
	}
	for priv := range ps.globalStatic {
		if _, ok := otherPs.globalStatic[priv]; !ok {
			return false
		}
	}
	for priv := range ps.globalDynamic {
		if _, ok := otherPs.globalDynamic[priv]; !ok {
			return false
		}
	}
	for dbName, dbSet := range ps.databases {
		if !dbSet.Equals(otherPs.databases[dbName]) {
			return false
		}
	}
	return true
}

// Copy returns a duplicate of the calling PrivilegeSet.
func (ps PrivilegeSet) Copy() PrivilegeSet {
	newPs := NewPrivilegeSet()
	newPs.UnionWith(ps)
	return newPs
}

// ToSlice returns all of the global static privileges contained as a slice. Some operations do not care about sort
// order, therefore this is presented as an alternative to skip the unnecessary sort operation.
func (ps PrivilegeSet) ToSlice() []sql.PrivilegeType {
	privs := make([]sql.PrivilegeType, len(ps.globalStatic))
	i := 0
	for priv := range ps.globalStatic {
		privs[i] = priv
		i++
	}
	return privs
}

// ToSortedSlice returns all of the global static privileges contained as a slice, sorted by their internal ID.
func (ps PrivilegeSet) ToSortedSlice() []sql.PrivilegeType {
	privs := ps.ToSlice()
	sort.Slice(privs, func(i, j int) bool {
		return privs[i] < privs[j]
	})
	return privs
}

// getUseableDb is used internally to either retrieve an existing database, or create a new one that is returned.
func (ps PrivilegeSet) getUseableDb(dbName string) PrivilegeSetDatabase {
	lowerDbName := strings.ToLower(dbName)
	dbSet, ok := ps.databases[lowerDbName]
	if !ok {
		dbSet = PrivilegeSetDatabase{
			name:   dbName,
			privs:  make(map[sql.PrivilegeType]struct{}),
			tables: make(map[string]PrivilegeSetTable),
		}
		ps.databases[lowerDbName] = dbSet
	}
	return dbSet
}

// PrivilegeSetDatabase is a set containing database-level privileges.
type PrivilegeSetDatabase struct {
	name   string
	privs  map[sql.PrivilegeType]struct{}
	tables map[string]PrivilegeSetTable
}

// Name returns the name of the database that this privilege set belongs to.
func (ps PrivilegeSetDatabase) Name() string {
	return ps.name
}

// Has returns whether the given database privilege(s) exists.
func (ps PrivilegeSetDatabase) Has(privileges ...sql.PrivilegeType) bool {
	for _, priv := range privileges {
		if _, ok := ps.privs[priv]; !ok {
			return false
		}
	}
	return true
}

// HasPrivileges returns whether this database has either database-level privileges, or privileges on a table or column
// contained within this database.
func (ps PrivilegeSetDatabase) HasPrivileges() bool {
	if len(ps.privs) > 0 {
		return true
	}
	for _, tblSet := range ps.tables {
		if tblSet.HasPrivileges() {
			return true
		}
	}
	return false
}

// Count returns the number of database privileges.
func (ps PrivilegeSetDatabase) Count() int {
	return len(ps.privs)
}

// Table returns the set of privileges for the given table. Returns an empty set if the table does not exist.
func (ps PrivilegeSetDatabase) Table(tblName string) PrivilegeSetTable {
	tblSet, ok := ps.tables[strings.ToLower(tblName)]
	if ok {
		return tblSet
	}
	return PrivilegeSetTable{name: tblName}
}

// GetTables returns all tables.
func (ps PrivilegeSetDatabase) GetTables() []PrivilegeSetTable {
	tblSets := make([]PrivilegeSetTable, len(ps.tables))
	i := 0
	for _, tblSet := range ps.tables {
		// Only return tables that have a table-level privilege, or a privilege on an underlying column.
		// Otherwise, there is no difference between the returned table and the zero-value for any table.
		if tblSet.HasPrivileges() {
			tblSets[i] = tblSet
			i++
		}
	}
	return tblSets
}

// Equals returns whether the given set of privileges is equivalent to the calling set.
func (ps PrivilegeSetDatabase) Equals(otherPs PrivilegeSetDatabase) bool {
	if len(ps.privs) != len(otherPs.privs) ||
		len(ps.tables) != len(otherPs.tables) {
		return false
	}
	for priv := range ps.privs {
		if _, ok := otherPs.privs[priv]; !ok {
			return false
		}
	}
	for tblName, tblSet := range ps.tables {
		if !tblSet.Equals(otherPs.tables[tblName]) {
			return false
		}
	}
	return true
}

// ToSlice returns all of the database privileges contained as a slice. Some operations do not care about sort order,
// therefore this is presented as an alternative to skip the unnecessary sort operation.
func (ps PrivilegeSetDatabase) ToSlice() []sql.PrivilegeType {
	privs := make([]sql.PrivilegeType, len(ps.privs))
	i := 0
	for priv := range ps.privs {
		privs[i] = priv
		i++
	}
	return privs
}

// ToSortedSlice returns all of the database privileges contained as a slice, sorted by their internal ID.
func (ps PrivilegeSetDatabase) ToSortedSlice() []sql.PrivilegeType {
	privs := ps.ToSlice()
	sort.Slice(privs, func(i, j int) bool {
		return privs[i] < privs[j]
	})
	return privs
}

// getUseableTbl is used internally to either retrieve an existing table, or create a new one that is returned.
func (ps PrivilegeSetDatabase) getUseableTbl(tblName string) PrivilegeSetTable {
	lowerTblName := strings.ToLower(tblName)
	tblSet, ok := ps.tables[lowerTblName]
	if !ok {
		tblSet = PrivilegeSetTable{
			name:    tblName,
			privs:   make(map[sql.PrivilegeType]struct{}),
			columns: make(map[string]PrivilegeSetColumn),
		}
		ps.tables[lowerTblName] = tblSet
	}
	return tblSet
}

// unionWith merges the given set of privileges to the calling set of privileges.
func (ps PrivilegeSetDatabase) unionWith(otherPs PrivilegeSetDatabase) {
	for priv := range otherPs.privs {
		ps.privs[priv] = struct{}{}
	}
	for _, otherTblSet := range otherPs.tables {
		ps.getUseableTbl(otherTblSet.name).unionWith(otherTblSet)
	}
}

// clear removes all database privileges.
func (ps PrivilegeSetDatabase) clear() {
	for priv := range ps.privs {
		delete(ps.privs, priv)
	}
}

// PrivilegeSetTable is a set containing table-level privileges.
type PrivilegeSetTable struct {
	name    string
	privs   map[sql.PrivilegeType]struct{}
	columns map[string]PrivilegeSetColumn
}

// Name returns the name of the table that this privilege set belongs to.
func (ps PrivilegeSetTable) Name() string {
	return ps.name
}

// Has returns whether the given table privilege(s) exists.
func (ps PrivilegeSetTable) Has(privileges ...sql.PrivilegeType) bool {
	for _, priv := range privileges {
		if _, ok := ps.privs[priv]; !ok {
			return false
		}
	}
	return true
}

// HasPrivileges returns whether this table has either table-level privileges, or privileges on a column contained
// within this table.
func (ps PrivilegeSetTable) HasPrivileges() bool {
	if len(ps.privs) > 0 {
		return true
	}
	for _, colSet := range ps.columns {
		if colSet.Count() > 0 {
			return true
		}
	}
	return false
}

// Count returns the number of table privileges.
func (ps PrivilegeSetTable) Count() int {
	return len(ps.privs)
}

// Column returns the set of privileges for the given column. Returns an empty set if the column does not exist.
func (ps PrivilegeSetTable) Column(colName string) PrivilegeSetColumn {
	colSet, ok := ps.columns[strings.ToLower(colName)]
	if ok {
		return colSet
	}
	return PrivilegeSetColumn{name: colName}
}

// GetColumns returns all columns.
func (ps PrivilegeSetTable) GetColumns() []PrivilegeSetColumn {
	colSets := make([]PrivilegeSetColumn, len(ps.columns))
	i := 0
	for _, colSet := range ps.columns {
		// Only return columns that have privileges. Otherwise, there is no difference between the returned column and
		// the zero-value for any column.
		if colSet.Count() > 0 {
			colSets[i] = colSet
			i++
		}
	}
	return colSets
}

// Equals returns whether the given set of privileges is equivalent to the calling set.
func (ps PrivilegeSetTable) Equals(otherPs PrivilegeSetTable) bool {
	if len(ps.privs) != len(otherPs.privs) ||
		len(ps.columns) != len(otherPs.columns) {
		return false
	}
	for priv := range ps.privs {
		if _, ok := otherPs.privs[priv]; !ok {
			return false
		}
	}
	for colName, colSet := range ps.columns {
		if !colSet.Equals(otherPs.columns[colName]) {
			return false
		}
	}
	return true
}

// ToSlice returns all of the table privileges contained as a slice. Some operations do not care about sort order,
// therefore this is presented as an alternative to skip the unnecessary sort operation.
func (ps PrivilegeSetTable) ToSlice() []sql.PrivilegeType {
	privs := make([]sql.PrivilegeType, len(ps.privs))
	i := 0
	for priv := range ps.privs {
		privs[i] = priv
		i++
	}
	return privs
}

// ToSortedSlice returns all of the table privileges contained as a slice, sorted by their internal ID.
func (ps PrivilegeSetTable) ToSortedSlice() []sql.PrivilegeType {
	privs := ps.ToSlice()
	sort.Slice(privs, func(i, j int) bool {
		return privs[i] < privs[j]
	})
	return privs
}

// getUseableCol is used internally to either retrieve an existing column, or create a new one that is returned.
func (ps PrivilegeSetTable) getUseableCol(colName string) PrivilegeSetColumn {
	lowerColName := strings.ToLower(colName)
	colSet, ok := ps.columns[lowerColName]
	if !ok {
		colSet = PrivilegeSetColumn{
			name:  colName,
			privs: make(map[sql.PrivilegeType]struct{}),
		}
		ps.columns[lowerColName] = colSet
	}
	return colSet
}

// unionWith merges the given set of privileges to the calling set of privileges.
func (ps PrivilegeSetTable) unionWith(otherPs PrivilegeSetTable) {
	for priv := range otherPs.privs {
		ps.privs[priv] = struct{}{}
	}
	for _, otherColSet := range otherPs.columns {
		ps.getUseableCol(otherColSet.name).unionWith(otherColSet)
	}
}

// clear removes all table privileges.
func (ps PrivilegeSetTable) clear() {
	for priv := range ps.privs {
		delete(ps.privs, priv)
	}
}

// PrivilegeSetColumn is a set containing column privileges.
type PrivilegeSetColumn struct {
	name  string
	privs map[sql.PrivilegeType]struct{}
}

// Name returns the name of the column that this privilege set belongs to.
func (ps PrivilegeSetColumn) Name() string {
	return ps.name
}

// Has returns whether the given column privilege(s) exists.
func (ps PrivilegeSetColumn) Has(privileges ...sql.PrivilegeType) bool {
	for _, priv := range privileges {
		if _, ok := ps.privs[priv]; !ok {
			return false
		}
	}
	return true
}

// Count returns the number of column privileges.
func (ps PrivilegeSetColumn) Count() int {
	return len(ps.privs)
}

// Equals returns whether the given set of privileges is equivalent to the calling set.
func (ps PrivilegeSetColumn) Equals(otherPs PrivilegeSetColumn) bool {
	if len(ps.privs) != len(otherPs.privs) {
		return false
	}
	for priv := range ps.privs {
		if _, ok := otherPs.privs[priv]; !ok {
			return false
		}
	}
	return true
}

// ToSlice returns all of the column privileges contained as a slice. Some operations do not care about sort order,
// therefore this is presented as an alternative to skip the unnecessary sort operation.
func (ps PrivilegeSetColumn) ToSlice() []sql.PrivilegeType {
	privs := make([]sql.PrivilegeType, len(ps.privs))
	i := 0
	for priv := range ps.privs {
		privs[i] = priv
		i++
	}
	return privs
}

// ToSortedSlice returns all of the column privileges contained as a slice, sorted by their internal ID.
func (ps PrivilegeSetColumn) ToSortedSlice() []sql.PrivilegeType {
	privs := ps.ToSlice()
	sort.Slice(privs, func(i, j int) bool {
		return privs[i] < privs[j]
	})
	return privs
}

// unionWith merges the given set of privileges to the calling set of privileges.
func (ps PrivilegeSetColumn) unionWith(otherPs PrivilegeSetColumn) {
	for priv := range otherPs.privs {
		ps.privs[priv] = struct{}{}
	}
}

// clear removes all column privileges.
func (ps PrivilegeSetColumn) clear() {
	for priv := range ps.privs {
		delete(ps.privs, priv)
	}
}
