// Copyright 2021 Dolthub, Inc.
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

package mysqlshim

import (
	"io"

	"github.com/gabereiser/go-mysql-server/sql"
)

// tablePartitionIter is used as a sql.PartitionIter.
type tablePartitionIter struct {
	returned bool
}

// tablePartition is used as a sql.Partition.
type tablePartition struct{}

var _ sql.PartitionIter = (*tablePartitionIter)(nil)

var _ sql.Partition = tablePartition{}

// Next implements the interface sql.PartitionIter.
func (t *tablePartitionIter) Next(*sql.Context) (sql.Partition, error) {
	if t.returned {
		return nil, io.EOF
	}
	t.returned = true
	return tablePartition{}, nil
}

// Close implements the interface sql.PartitionIter.
func (t *tablePartitionIter) Close(ctx *sql.Context) error {
	return nil
}

// Key implements the interface sql.Partition.
func (t tablePartition) Key() []byte {
	return nil
}
