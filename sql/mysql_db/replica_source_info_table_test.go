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

import "testing"

func TestReplicaSourceInfoTableSchema(t *testing.T) {
	// Each column has a constant index that it expects to match, therefore if a column's position is updated and the
	// variable referencing it hasn't also been updated, this will throw a panic.
	for i, col := range replicaSourceInfoTblSchema {
		switch col.Name {
		case "Number_of_lines":
			if replicaSourceInfoTblColIndex_Number_of_lines != i {
				t.FailNow()
			}
		case "Master_log_name":
			if replicaSourceInfoTblColIndex_Master_log_name != i {
				t.FailNow()
			}
		case "Master_log_pos":
			if replicaSourceInfoTblColIndex_Master_log_pos != i {
				t.FailNow()
			}
		case "Host":
			if replicaSourceInfoTblColIndex_Host != i {
				t.FailNow()
			}
		case "User_name":
			if replicaSourceInfoTblColIndex_User_name != i {
				t.FailNow()
			}
		case "User_password":
			if replicaSourceInfoTblColIndex_User_password != i {
				t.FailNow()
			}
		case "Port":
			if replicaSourceInfoTblColIndex_Port != i {
				t.FailNow()
			}
		case "Connect_retry":
			if replicaSourceInfoTblColIndex_Connect_retry != i {
				t.FailNow()
			}
		case "Enabled_ssl":
			if replicaSourceInfoTblColIndex_Enabled_ssl != i {
				t.FailNow()
			}
		case "Ssl_ca":
			if replicaSourceInfoTblColIndex_Ssl_ca != i {
				t.FailNow()
			}
		case "Ssl_capath":
			if replicaSourceInfoTblColIndex_Ssl_capath != i {
				t.FailNow()
			}
		case "Ssl_cert":
			if replicaSourceInfoTblColIndex_Ssl_cert != i {
				t.FailNow()
			}
		case "Ssl_cipher":
			if replicaSourceInfoTblColIndex_Ssl_cipher != i {
				t.FailNow()
			}
		case "Ssl_key":
			if replicaSourceInfoTblColIndex_Ssl_key != i {
				t.FailNow()
			}
		case "Ssl_verify_server_cert":
			if replicaSourceInfoTblColIndex_Ssl_verify_server_cert != i {
				t.FailNow()
			}
		case "Heartbeat":
			if replicaSourceInfoTblColIndex_Heartbeat != i {
				t.FailNow()
			}
		case "Bind":
			if replicaSourceInfoTblColIndex_Bind != i {
				t.FailNow()
			}
		case "Ignored_server_ids":
			if replicaSourceInfoTblColIndex_Ignored_server_ids != i {
				t.FailNow()
			}
		case "Uuid":
			if replicaSourceInfoTblColIndex_Uuid != i {
				t.FailNow()
			}
		case "Retry_count":
			if replicaSourceInfoTblColIndex_Retry_count != i {
				t.FailNow()
			}
		case "Ssl_crl":
			if replicaSourceInfoTblColIndex_Ssl_crl != i {
				t.FailNow()
			}
		case "Ssl_crlpath":
			if replicaSourceInfoTblColIndex_Ssl_crlpath != i {
				t.FailNow()
			}
		case "Enabled_auto_position":
			if replicaSourceInfoTblColIndex_Enabled_auto_position != i {
				t.FailNow()
			}
		case "Channel_name":
			if replicaSourceInfoTblColIndex_Channel_name != i {
				t.FailNow()
			}
		case "Tls_version":
			if replicaSourceInfoTblColIndex_Tls_version != i {
				t.FailNow()
			}
		case "Public_key_path":
			if replicaSourceInfoTblColIndex_Public_key_path != i {
				t.FailNow()
			}
		case "Get_public_key":
			if replicaSourceInfoTblColIndex_Get_public_key != i {
				t.FailNow()
			}
		case "Network_namespace":
			if replicaSourceInfoTblColIndex_Network_namespace != i {
				t.FailNow()
			}
		case "Master_compression_algorithm":
			if replicaSourceInfoTblColIndex_Master_compression_algorithm != i {
				t.FailNow()
			}
		case "Master_zstd_compression_level":
			if replicaSourceInfoTblColIndex_Master_zstd_compression_level != i {
				t.FailNow()
			}
		case "Tls_ciphersuites":
			if replicaSourceInfoTblColIndex_Tls_ciphersuites != i {
				t.FailNow()
			}
		case "Source_connection_auto_failover":
			if replicaSourceInfoTblColIndex_Source_connection_auto_failover != i {
				t.FailNow()
			}
		case "Gtid_only":
			if replicaSourceInfoTblColIndex_Gtid_only != i {
				t.FailNow()
			}
		default:
			t.Errorf(`col "%s" does not have a constant`, col.Name)
		}
	}
}
