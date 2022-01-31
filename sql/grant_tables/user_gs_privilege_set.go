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

type UserGlobalStaticPrivileges struct {
	privSet map[PrivilegeType]struct{}
}

// NewUserGlobalStaticPrivileges returns a new UserGlobalStaticPrivileges.
func NewUserGlobalStaticPrivileges() UserGlobalStaticPrivileges {
	return UserGlobalStaticPrivileges{make(map[PrivilegeType]struct{})}
}

// newUserGlobalStaticPrivilegesWithAllPrivileges returns a new UserGlobalStaticPrivileges with every global static
// privilege added.
func newUserGlobalStaticPrivilegesWithAllPrivileges() UserGlobalStaticPrivileges {
	return UserGlobalStaticPrivileges{
		map[PrivilegeType]struct{}{
			PrivilegeType_Select:            {},
			PrivilegeType_Insert:            {},
			PrivilegeType_Update:            {},
			PrivilegeType_Delete:            {},
			PrivilegeType_Create:            {},
			PrivilegeType_Drop:              {},
			PrivilegeType_Reload:            {},
			PrivilegeType_Shutdown:          {},
			PrivilegeType_Process:           {},
			PrivilegeType_File:              {},
			PrivilegeType_Grant:             {},
			PrivilegeType_References:        {},
			PrivilegeType_Index:             {},
			PrivilegeType_Alter:             {},
			PrivilegeType_ShowDB:            {},
			PrivilegeType_Super:             {},
			PrivilegeType_CreateTempTable:   {},
			PrivilegeType_LockTables:        {},
			PrivilegeType_Execute:           {},
			PrivilegeType_ReplicationSlave:  {},
			PrivilegeType_ReplicationClient: {},
			PrivilegeType_CreateView:        {},
			PrivilegeType_ShowView:          {},
			PrivilegeType_CreateRoutine:     {},
			PrivilegeType_AlterRoutine:      {},
			PrivilegeType_CreateUser:        {},
			PrivilegeType_Event:             {},
			PrivilegeType_Trigger:           {},
			PrivilegeType_CreateTablespace:  {},
			PrivilegeType_CreateRole:        {},
			PrivilegeType_DropRole:          {},
		},
	}
}

// Add adds the given privilege(s).
func (ugsp UserGlobalStaticPrivileges) Add(privileges ...PrivilegeType) {
	for _, priv := range privileges {
		ugsp.privSet[priv] = struct{}{}
	}
}

// Remove removes the given privilege(s).
func (ugsp UserGlobalStaticPrivileges) Remove(privileges ...PrivilegeType) {
	for _, priv := range privileges {
		delete(ugsp.privSet, priv)
	}
}

// Has returns whether the given privilege(s) exists.
func (ugsp UserGlobalStaticPrivileges) Has(privileges ...PrivilegeType) bool {
	for _, priv := range privileges {
		if _, ok := ugsp.privSet[priv]; !ok {
			return false
		}
	}
	return true
}

// Equals returns whether the given set of privileges is equivalent to the calling set.
func (ugsp UserGlobalStaticPrivileges) Equals(otherUgsp UserGlobalStaticPrivileges) bool {
	if len(ugsp.privSet) != len(otherUgsp.privSet) {
		return false
	}
	for priv := range ugsp.privSet {
		if _, ok := otherUgsp.privSet[priv]; !ok {
			return false
		}
	}
	return true
}

// Len returns the number of privileges contained within.
func (ugsp UserGlobalStaticPrivileges) Len() int {
	return len(ugsp.privSet)
}

// ToSlice returns all of the privileges contained as a slice.
func (ugsp UserGlobalStaticPrivileges) ToSlice() []PrivilegeType {
	privs := make([]PrivilegeType, len(ugsp.privSet))
	i := 0
	for priv := range ugsp.privSet {
		privs[i] = priv
		i++
	}
	return privs
}
