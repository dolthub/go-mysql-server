// Copyright 2023 Dolthub, Inc.
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
	"encoding/json"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
)

// ReplicaSourceInfo represents
type ReplicaSourceInfo struct {
	Host     string
	User     string
	Password string
	Port     uint16
}

var _ in_mem_table.Entry = (*ReplicaSourceInfo)(nil)

// NewFromRow implements the interface in_mem_table.Entry.
func (r *ReplicaSourceInfo) NewFromRow(_ *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	if err := userTblSchema.CheckRow(row); err != nil {
		return nil, err
	}

	return &ReplicaSourceInfo{
		Host:     row[replicaSourceInfoTblColIndex_Host].(string),
		User:     row[replicaSourceInfoTblColIndex_User_name].(string),
		Password: row[replicaSourceInfoTblColIndex_User_password].(string),
		Port:     row[replicaSourceInfoTblColIndex_Port].(uint16),
	}, nil
}

// UpdateFromRow implements the interface in_mem_table.Entry.
func (r *ReplicaSourceInfo) UpdateFromRow(ctx *sql.Context, row sql.Row) (in_mem_table.Entry, error) {
	updatedEntry, err := r.NewFromRow(ctx, row)
	if err != nil {
		return nil, err
	}
	return updatedEntry, nil
}

// ToRow implements the interface in_mem_table.Entry.
func (r *ReplicaSourceInfo) ToRow(ctx *sql.Context) sql.Row {
	row := make(sql.Row, len(replicaSourceInfoTblSchema))
	var err error
	for i, col := range replicaSourceInfoTblSchema {
		row[i], err = col.Default.Eval(ctx, nil)
		if err != nil {
			panic(err) // Should never happen, schema is static
		}
	}
	//TODO: once the remaining fields are added, fill those in as well
	row[replicaSourceInfoTblColIndex_Host] = r.Host
	row[replicaSourceInfoTblColIndex_User_name] = r.User
	row[replicaSourceInfoTblColIndex_User_password] = r.Password
	row[replicaSourceInfoTblColIndex_Port] = r.Port
	return row
}

// Equals implements the interface in_mem_table.Entry.
func (r *ReplicaSourceInfo) Equals(_ *sql.Context, otherEntry in_mem_table.Entry) bool {
	other, ok := otherEntry.(*ReplicaSourceInfo)
	if !ok {
		return false
	}

	//TODO: once the remaining fields are added, fill those in as well
	if r.User != other.User ||
		r.Host != other.Host ||
		r.Port != other.Port ||
		r.Password != other.Password {
		return false
	}

	return true
}

// Copy implements the interface in_mem_table.Entry.
func (r *ReplicaSourceInfo) Copy(_ *sql.Context) in_mem_table.Entry {
	rr := *r
	return &rr
}

// FromJson implements the interface in_mem_table.Entry.
func (r *ReplicaSourceInfo) FromJson(_ *sql.Context, jsonStr string) (in_mem_table.Entry, error) {
	newInstance := &ReplicaSourceInfo{}
	if err := json.Unmarshal([]byte(jsonStr), newInstance); err != nil {
		return nil, err
	}
	return newInstance, nil
}

// ToJson implements the interface in_mem_table.Entry.
func (r *ReplicaSourceInfo) ToJson(_ *sql.Context) (string, error) {
	jsonData, err := json.Marshal(*r)
	if err != nil {
		return "", err
	}
	return string(jsonData), nil
}
