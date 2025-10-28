// Copyright 2025 Dolthub, Inc.
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

package plan

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// DynamicPrivilege_BinlogAdmin enables binary log control by means of the PURGE BINARY LOGS and BINLOG statements.
// https://dev.mysql.com/doc/refman/8.0/en/privileges-provided.html#priv_binlog-admin
const DynamicPrivilege_BinlogAdmin = "binlog_admin"

// Binlog replays binary log events, which record database changes in a binary format for efficiency. Tools like
// mysqldump, mysqlbinlog, and mariadb-binlog read these binary events from log files and output them as base64-encoded
// BINLOG statements for replay.
//
// This implementation supports row-based replication (RBR) events, which is the modern standard for MySQL/MariaDB
// replication (default since MySQL 5.7+ and MariaDB 10.2+).
//
// The base64 string is split by newlines and decoded into a buffer of raw binlog events. Each event begins with a
// 19-byte header containing the event type at byte 4 and event length at bytes 9-12. The buffer is processed
// sequentially, dispatching each event to a handler based on its type.
//
// FORMAT_DESCRIPTION_EVENT stores binlog format metadata in session state. This metadata includes header sizes for each
// event type and the checksum algorithm (OFF, CRC32, or UNDEF). Subsequent events require this metadata to parse their
// headers and determine whether checksums are present.
//
// TABLE_MAP_EVENT creates a mapping from a table ID to the database name, table name, and column metadata. Row events
// reference tables by ID rather than name for encoding efficiency. The mapping is stored in a global cache for use by
// subsequent row events.
//
// WRITE_ROWS_EVENT, UPDATE_ROWS_EVENT, and DELETE_ROWS_EVENT contain binary-encoded row data. Before parsing row data,
// any CRC32 checksum appended to the event must be stripped. Checksums verify data integrity during network
// transmission and disk storage but are not part of the event payload structure.
//
// Transaction boundary and metadata events are silently ignored since each BINLOG statement is auto-committed and full
// replication semantics are not required for mysqldump replay.
//
// QUERY_EVENT (statement-based replication) is not currently supported. Statement-based replication is deprecated in
// favor of row-based replication to correctly support non-deterministic functions.
//
// See https://dev.mysql.com/doc/refman/8.4/en/binlog.html for the BINLOG statement specification.
type Binlog struct {
	Base64Str string
	Catalog   sql.Catalog
}

var _ sql.Node = (*Binlog)(nil)

// NewBinlog creates a new Binlog node.
func NewBinlog(base64Str string, catalog sql.Catalog) *Binlog {
	return &Binlog{
		Base64Str: base64Str,
		Catalog:   catalog,
	}
}

func (b *Binlog) String() string {
	return "BINLOG"
}

func (b *Binlog) Resolved() bool {
	return true
}

func (b *Binlog) Schema() sql.Schema {
	return types.OkResultSchema
}

func (b *Binlog) Children() []sql.Node {
	return nil
}

func (b *Binlog) IsReadOnly() bool {
	return false
}

// WithChildren implements the Node interface.
func (b *Binlog) WithChildren(children ...sql.Node) (sql.Node, error) {
	if len(children) != 0 {
		return nil, sql.ErrInvalidChildrenNumber.New(b, len(children), 0)
	}
	return b, nil
}
