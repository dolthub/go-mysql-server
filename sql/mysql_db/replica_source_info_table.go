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
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/in_mem_table"
	"github.com/dolthub/vitess/go/sqltypes"
)

// replicaSourceInfoTblName stores the name of the mysql table for persistent storage
// of replication data.
// For more details, see: https://dev.mysql.com/doc/refman/8.0/en/replica-logs-status.html
const replicaSourceInfoTblName = "slave_master_info"

var replicaSourceInfoTblSchema sql.Schema

type ReplicaSourceInfoPrimaryKey struct {
	Channel string
}

var _ in_mem_table.Key = ReplicaSourceInfoPrimaryKey{}

// KeyFromEntry implements the interface in_mem_table.Key.
func (r ReplicaSourceInfoPrimaryKey) KeyFromEntry(_ *sql.Context, entry in_mem_table.Entry) (in_mem_table.Key, error) {
	_, ok := entry.(*ReplicaSourceInfo)
	if !ok {
		return nil, errPrimaryKeyUnknownEntry.New(replicaSourceInfoTblName)
	}
	return ReplicaSourceInfoPrimaryKey{
		Channel: "",
	}, nil
}

// KeyFromRow implements the interface in_mem_table.Key.
func (r ReplicaSourceInfoPrimaryKey) KeyFromRow(_ *sql.Context, row sql.Row) (in_mem_table.Key, error) {
	if len(row) != len(replicaSourceInfoTblSchema) {
		return r, errPrimaryKeyUnknownSchema.New(replicaSourceInfoTblName)
	}
	return ReplicaSourceInfoPrimaryKey{
		Channel: "",
	}, nil
}

func init() {
	char255_ascii_general_ci := sql.MustCreateString(sqltypes.Char, 255, sql.Collation_ascii_general_ci)
	char64_utf8mb3_bin := sql.MustCreateString(sqltypes.Char, 64, sql.Collation_utf8mb3_bin)
	char64_utf8mb3_general_ci := sql.MustCreateString(sqltypes.Char, 64, sql.Collation_utf8mb3_general_ci)

	replicaSourceInfoTblSchema = sql.Schema{
		columnTemplate("Number_of_lines", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Uint32,
		}),
		columnTemplate("Master_log_name", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Text,
		}),
		columnTemplate("Master_log_pos", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Uint64,
		}),
		columnTemplate("Host", replicaSourceInfoTblName, false, &sql.Column{
			Type:     char255_ascii_general_ci,
			Nullable: true,
		}),
		columnTemplate("User_name", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("User_password", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Port", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Uint32,
		}),
		columnTemplate("Connect_retry", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Uint32,
		}),
		columnTemplate("Enabled_ssl", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Int8,
		}),
		columnTemplate("Ssl_ca", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Ssl_capath", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Ssl_cert", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Ssl_cipher", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Ssl_key", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Ssl_verify_server_cert", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Int8,
		}),
		columnTemplate("Heartbeat", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Float32,
		}),
		columnTemplate("Bind", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Ignored_server_ids", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Uuid", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Retry_count", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Uint64,
		}),
		columnTemplate("Ssl_crl", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Ssl_crlpath", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Enabled_auto_position", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Int8,
		}),
		columnTemplate("Channel_name", replicaSourceInfoTblName, true, &sql.Column{
			Type: char64_utf8mb3_general_ci,
		}),
		columnTemplate("Tls_version", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Public_key_path", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Get_public_key", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Int8,
		}),
		columnTemplate("Network_namespace", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Master_compression_algorithm", replicaSourceInfoTblName, false, &sql.Column{
			Type: char64_utf8mb3_bin,
		}),
		columnTemplate("Master_zstd_compression_level", replicaSourceInfoTblName, false, &sql.Column{
			Type: sql.Uint32,
		}),
		columnTemplate("Tls_ciphersuites", replicaSourceInfoTblName, false, &sql.Column{
			Type:     sql.Text,
			Nullable: true,
		}),
		columnTemplate("Source_connection_auto_failover", replicaSourceInfoTblName, false, &sql.Column{
			Type:    sql.Int8,
			Default: mustDefault(expression.NewLiteral(0, sql.Int8), sql.Int8, true, false),
		}),
		columnTemplate("Gtid_only", replicaSourceInfoTblName, false, &sql.Column{
			Type:    sql.Int8,
			Default: mustDefault(expression.NewLiteral(0, sql.Int8), sql.Int8, true, false),
		}),
	}
}

const (
	replicaSourceInfoTblColIndex_Number_of_lines int = iota
	replicaSourceInfoTblColIndex_Master_log_name
	replicaSourceInfoTblColIndex_Master_log_pos
	replicaSourceInfoTblColIndex_Host
	replicaSourceInfoTblColIndex_User_name
	replicaSourceInfoTblColIndex_User_password
	replicaSourceInfoTblColIndex_Port
	replicaSourceInfoTblColIndex_Connect_retry
	replicaSourceInfoTblColIndex_Enabled_ssl
	replicaSourceInfoTblColIndex_Ssl_ca
	replicaSourceInfoTblColIndex_Ssl_capath
	replicaSourceInfoTblColIndex_Ssl_cert
	replicaSourceInfoTblColIndex_Ssl_cipher
	replicaSourceInfoTblColIndex_Ssl_key
	replicaSourceInfoTblColIndex_Ssl_verify_server_cert
	replicaSourceInfoTblColIndex_Heartbeat
	replicaSourceInfoTblColIndex_Bind
	replicaSourceInfoTblColIndex_Ignored_server_ids
	replicaSourceInfoTblColIndex_Uuid
	replicaSourceInfoTblColIndex_Retry_count
	replicaSourceInfoTblColIndex_Ssl_crl
	replicaSourceInfoTblColIndex_Ssl_crlpath
	replicaSourceInfoTblColIndex_Enabled_auto_position
	replicaSourceInfoTblColIndex_Channel_name
	replicaSourceInfoTblColIndex_Tls_version
	replicaSourceInfoTblColIndex_Public_key_path
	replicaSourceInfoTblColIndex_Get_public_key
	replicaSourceInfoTblColIndex_Network_namespace
	replicaSourceInfoTblColIndex_Master_compression_algorithm
	replicaSourceInfoTblColIndex_Master_zstd_compression_level
	replicaSourceInfoTblColIndex_Tls_ciphersuites
	replicaSourceInfoTblColIndex_Source_connection_auto_failover
	replicaSourceInfoTblColIndex_Gtid_only
)
