// Copyright 2024 Dolthub, Inc.
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

package variables

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// globalStatusVariables is the concrete status variable registry.
type globalStatusVariables struct {
	varVals map[string]sql.StatusVarValue
}

var _ sql.StatusVariableRegistry = (*globalStatusVariables)(nil)

// NewStatusVariableRegistry returns fresh status definitions and counters.
func NewStatusVariableRegistry() sql.StatusVariableRegistry {
	return newStatusVariableRegistry(newStatusVars())
}

func newStatusVariableRegistry(definitions map[string]sql.StatusVariable) *globalStatusVariables {
	registry := &globalStatusVariables{varVals: make(map[string]sql.StatusVarValue, len(definitions))}
	for _, variable := range definitions {
		registry.varVals[variable.GetName()] = variable.NewValue()
	}
	return registry
}

// NewSessionMap implements sql.StatusVariableRegistry
func (g *globalStatusVariables) NewSessionMap() map[string]sql.StatusVarValue {
	sessionMap := make(map[string]sql.StatusVarValue, len(g.varVals))
	for k, v := range g.varVals {
		if v.Variable().GetScope() == sql.StatusVariableScope_Global {
			continue
		}
		sessionMap[k] = v.Variable().NewValue()
	}
	return sessionMap
}

// NewGlobalMap implements sql.StatusVariableRegistry
func (g *globalStatusVariables) NewGlobalMap() map[string]sql.StatusVarValue {
	globalMap := make(map[string]sql.StatusVarValue, len(g.varVals))
	for k, v := range g.varVals {
		if v.Variable().GetScope() == sql.StatusVariableScope_Session {
			continue
		}
		globalMap[k] = v.Copy()
	}
	return globalMap
}

// GetGlobal implements sql.StatusVariableRegistry
func (g *globalStatusVariables) GetGlobal(name string) (sql.StatusVariable, interface{}, bool) {
	v, ok := g.varVals[name]
	if !ok || v.Variable().GetScope() == sql.StatusVariableScope_Session {
		return nil, nil, false
	}
	return v.Variable(), v.Value(), true
}

// SetGlobal implements sql.StatusVariableRegistry
func (g *globalStatusVariables) SetGlobal(name string, val interface{}) error {
	v, ok := g.varVals[name]
	if !ok || v.Variable().GetScope() == sql.StatusVariableScope_Session {
		return sql.ErrUnknownStatusVariable.New(name)
	}

	// TODO: We're currently hardcoding a Uint64 conversion here, which prevents us from supporting
	//       non-uint64 status variables. This should really use the type of the status variable as
	//       type to convert to, but because those are systemInt without the correct bounds configured,
	//       that conversion will fail with an out of bounds error.
	// NOTE: A nil context is passed in here. A context is only needed for adaptive encoding, which shouldn't be possible here.
	convVal, _, err := types.Uint64.Convert(nil, val)
	if err != nil {
		return err
	}
	g.varVals[name].Set(convVal)
	return nil
}

// IncrementGlobal implements sql.StatusVariableRegistry
func (g *globalStatusVariables) IncrementGlobal(name string, val int) {
	v, ok := g.varVals[name]
	if !ok || v.Variable().GetScope() == sql.StatusVariableScope_Session {
		return
	}
	g.varVals[name].Increment(uint64(val))
	return
}

func newStatusVars() map[string]sql.StatusVariable {
	return map[string]sql.StatusVariable{
		"Aborted_clients": &sql.MySQLStatusCounter{
			Name:  "Aborted_clients",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Aborted_clients", 0, 0, false),
		},
		"Aborted_connects": &sql.MySQLStatusCounter{
			Name:  "Aborted_connects",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Aborted_connects", 0, 0, false),
		},
		"Acl_cache_items_count": &sql.MySQLStatusCounter{
			Name:  "Acl_cache_items_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Acl_cache_items_count", 0, 0, false),
		},
		"Audit_log_current_size": &sql.MySQLStatusCounter{
			Name:  "Audit_log_current_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_current_size", 0, 0, false),
		},
		"Audit_log_event_max_drop_size": &sql.MySQLStatusCounter{
			Name:  "Audit_log_event_max_drop_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_event_max_drop_size", 0, 0, false),
		},
		"Audit_log_events": &sql.MySQLStatusCounter{
			Name:  "Audit_log_events",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_events", 0, 0, false),
		},
		"Audit_log_events_filtered": &sql.MySQLStatusCounter{
			Name:  "Audit_log_events_filtered",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_events_filtered", 0, 0, false),
		},
		"Audit_log_events_lost": &sql.MySQLStatusCounter{
			Name:  "Audit_log_events_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_events_lost", 0, 0, false),
		},
		"Audit_log_events_written": &sql.MySQLStatusCounter{
			Name:  "Audit_log_events_written",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_events_written", 0, 0, false),
		},
		"Audit_log_total_size": &sql.MySQLStatusCounter{
			Name:  "Audit_log_total_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_total_size", 0, 0, false),
		},
		"Audit_log_write_waits": &sql.MySQLStatusCounter{
			Name:  "Audit_log_write_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Audit_log_write_waits", 0, 0, false),
		},
		"Authentication_ldap_sasl_supported_methods": &sql.MySQLStatusVariable{
			Name:    "Authentication_ldap_sasl_supported_methods",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Authentication_ldap_sasl_supported_methods"),
			Default: "",
		},
		"Binlog_cache_disk_use": &sql.MySQLStatusCounter{
			Name:  "Binlog_cache_disk_use",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Binlog_cache_disk_use", 0, 0, false),
		},
		"Binlog_cache_use": &sql.MySQLStatusCounter{
			Name:  "Binlog_cache_use",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Binlog_cache_use", 0, 0, false),
		},
		"Binlog_stmt_cache_disk_use": &sql.MySQLStatusCounter{
			Name:  "Binlog_stmt_cache_disk_use",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Binlog_stmt_cache_disk_use", 0, 0, false),
		},
		"Binlog_stmt_cache_use": &sql.MySQLStatusCounter{
			Name:  "Binlog_stmt_cache_use",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Binlog_stmt_cache_use", 0, 0, false),
		},
		"Bytes_received": &sql.MySQLStatusCounter{
			Name:  "Bytes_received",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Bytes_received", 0, 0, false),
		},
		"Bytes_sent": &sql.MySQLStatusCounter{
			Name:  "Bytes_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Bytes_sent", 0, 0, false),
		},
		"Caching_sha2_password_rsa_public_key": &sql.MySQLStatusVariable{
			Name:    "Caching_sha2_password_rsa_public_key",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Caching_sha2_password_rsa_public_key"),
			Default: "",
		},
		"Com_admin_commands": &sql.MySQLStatusCounter{
			Name:  "Com_admin_commands",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_admin_commands", 0, 0, false),
		},
		"Com_alter_db": &sql.MySQLStatusCounter{
			Name:  "Com_alter_db",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_db", 0, 0, false),
		},
		"Com_alter_event": &sql.MySQLStatusCounter{
			Name:  "Com_alter_event",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_event", 0, 0, false),
		},
		"Com_alter_function": &sql.MySQLStatusCounter{
			Name:  "Com_alter_function",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_function", 0, 0, false),
		},
		"Com_alter_procedure": &sql.MySQLStatusCounter{
			Name:  "Com_alter_procedure",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_procedure", 0, 0, false),
		},
		"Com_alter_resource_group": &sql.MySQLStatusCounter{
			Name:  "Com_alter_resource_group",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_alter_resource_group", 0, 0, false),
		},
		"Com_alter_server": &sql.MySQLStatusCounter{
			Name:  "Com_alter_server",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_server", 0, 0, false),
		},
		"Com_alter_table": &sql.MySQLStatusCounter{
			Name:  "Com_alter_table",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_table", 0, 0, false),
		},
		"Com_alter_tablespace": &sql.MySQLStatusCounter{
			Name:  "Com_alter_tablespace",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_tablespace", 0, 0, false),
		},
		"Com_alter_user": &sql.MySQLStatusCounter{
			Name:  "Com_alter_user",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_alter_user", 0, 0, false),
		},
		"Com_alter_user_default_role": &sql.MySQLStatusCounter{
			Name:  "Com_alter_user_default_role",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_alter_user_default_role", 0, 0, false),
		},
		"Com_analyze": &sql.MySQLStatusCounter{
			Name:  "Com_analyze",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_analyze", 0, 0, false),
		},
		"Com_assign_to_keycache": &sql.MySQLStatusCounter{
			Name:  "Com_assign_to_keycache",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_assign_to_keycache", 0, 0, false),
		},
		"Com_begin": &sql.MySQLStatusCounter{
			Name:  "Com_begin",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_begin", 0, 0, false),
		},
		"Com_binlog": &sql.MySQLStatusCounter{
			Name:  "Com_binlog",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_binlog", 0, 0, false),
		},
		"Com_call_procedure": &sql.MySQLStatusCounter{
			Name:  "Com_call_procedure",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_call_procedure", 0, 0, false),
		},
		"Com_change_db": &sql.MySQLStatusCounter{
			Name:  "Com_change_db",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_change_db", 0, 0, false),
		},
		"Com_change_master": &sql.MySQLStatusCounter{
			Name:  "Com_change_master",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_change_master", 0, 0, false),
		},
		"Com_change_repl_filter": &sql.MySQLStatusCounter{
			Name:  "Com_change_repl_filter",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_change_repl_filter", 0, 0, false),
		},
		"Com_change_replication_source": &sql.MySQLStatusCounter{
			Name:  "Com_change_replication_source",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_change_replication_source", 0, 0, false),
		},
		"Com_check": &sql.MySQLStatusCounter{
			Name:  "Com_check",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_check", 0, 0, false),
		},
		"Com_checksum": &sql.MySQLStatusCounter{
			Name:  "Com_checksum",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_checksum", 0, 0, false),
		},
		"Com_clone": &sql.MySQLStatusCounter{
			Name:  "Com_clone",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_clone", 0, 0, false),
		},
		"Com_commit": &sql.MySQLStatusCounter{
			Name:  "Com_commit",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_commit", 0, 0, false),
		},
		"Com_create_db": &sql.MySQLStatusCounter{
			Name:  "Com_create_db",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_db", 0, 0, false),
		},
		"Com_create_event": &sql.MySQLStatusCounter{
			Name:  "Com_create_event",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_event", 0, 0, false),
		},
		"Com_create_function": &sql.MySQLStatusCounter{
			Name:  "Com_create_function",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_function", 0, 0, false),
		},
		"Com_create_index": &sql.MySQLStatusCounter{
			Name:  "Com_create_index",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_index", 0, 0, false),
		},
		"Com_create_procedure": &sql.MySQLStatusCounter{
			Name:  "Com_create_procedure",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_procedure", 0, 0, false),
		},
		"Com_create_resource_group": &sql.MySQLStatusCounter{
			Name:  "Com_create_resource_group",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_create_resource_group", 0, 0, false),
		},
		"Com_create_role": &sql.MySQLStatusCounter{
			Name:  "Com_create_role",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_create_role", 0, 0, false),
		},
		"Com_create_server": &sql.MySQLStatusCounter{
			Name:  "Com_create_server",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_server", 0, 0, false),
		},
		"Com_create_table": &sql.MySQLStatusCounter{
			Name:  "Com_create_table",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_table", 0, 0, false),
		},
		"Com_create_trigger": &sql.MySQLStatusCounter{
			Name:  "Com_create_trigger",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_trigger", 0, 0, false),
		},
		"Com_create_udf": &sql.MySQLStatusCounter{
			Name:  "Com_create_udf",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_udf", 0, 0, false),
		},
		"Com_create_user": &sql.MySQLStatusCounter{
			Name:  "Com_create_user",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_user", 0, 0, false),
		},
		"Com_create_view": &sql.MySQLStatusCounter{
			Name:  "Com_create_view",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_create_view", 0, 0, false),
		},
		"Com_dealloc_sql": &sql.MySQLStatusCounter{
			Name:  "Com_dealloc_sql",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_dealloc_sql", 0, 0, false),
		},
		"Com_delete": &sql.MySQLStatusCounter{
			Name:  "Com_delete",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_delete", 0, 0, false),
		},
		"Com_delete_multi": &sql.MySQLStatusCounter{
			Name:  "Com_delete_multi",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_delete_multi", 0, 0, false),
		},
		"Com_do": &sql.MySQLStatusCounter{
			Name:  "Com_do",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_do", 0, 0, false),
		},
		"Com_drop_db": &sql.MySQLStatusCounter{
			Name:  "Com_drop_db",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_db", 0, 0, false),
		},
		"Com_drop_event": &sql.MySQLStatusCounter{
			Name:  "Com_drop_event",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_event", 0, 0, false),
		},
		"Com_drop_function": &sql.MySQLStatusCounter{
			Name:  "Com_drop_function",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_function", 0, 0, false),
		},
		"Com_drop_index": &sql.MySQLStatusCounter{
			Name:  "Com_drop_index",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_index", 0, 0, false),
		},
		"Com_drop_procedure": &sql.MySQLStatusCounter{
			Name:  "Com_drop_procedure",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_procedure", 0, 0, false),
		},
		"Com_drop_resource_group": &sql.MySQLStatusCounter{
			Name:  "Com_drop_resource_group",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_drop_resource_group", 0, 0, false),
		},
		"Com_drop_role": &sql.MySQLStatusCounter{
			Name:  "Com_drop_role",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_drop_role", 0, 0, false),
		},
		"Com_drop_server": &sql.MySQLStatusCounter{
			Name:  "Com_drop_server",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_server", 0, 0, false),
		},
		"Com_drop_table": &sql.MySQLStatusCounter{
			Name:  "Com_drop_table",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_table", 0, 0, false),
		},
		"Com_drop_trigger": &sql.MySQLStatusCounter{
			Name:  "Com_drop_trigger",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_trigger", 0, 0, false),
		},
		"Com_drop_user": &sql.MySQLStatusCounter{
			Name:  "Com_drop_user",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_user", 0, 0, false),
		},
		"Com_drop_view": &sql.MySQLStatusCounter{
			Name:  "Com_drop_view",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_drop_view", 0, 0, false),
		},
		"Com_empty_query": &sql.MySQLStatusCounter{
			Name:  "Com_empty_query",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_empty_query", 0, 0, false),
		},
		"Com_execute_sql": &sql.MySQLStatusCounter{
			Name:  "Com_execute_sql",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_execute_sql", 0, 0, false),
		},
		"Com_explain_other": &sql.MySQLStatusCounter{
			Name:  "Com_explain_other",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_explain_other", 0, 0, false),
		},
		"Com_flush": &sql.MySQLStatusCounter{
			Name:  "Com_flush",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_flush", 0, 0, false),
		},
		"Com_get_diagnostics": &sql.MySQLStatusCounter{
			Name:  "Com_get_diagnostics",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_get_diagnostics", 0, 0, false),
		},
		"Com_grant": &sql.MySQLStatusCounter{
			Name:  "Com_grant",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_grant", 0, 0, false),
		},
		"Com_grant_roles": &sql.MySQLStatusCounter{
			Name:  "Com_grant_roles",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_grant_roles", 0, 0, false),
		},
		"Com_group_replication_start": &sql.MySQLStatusCounter{
			Name:  "Com_group_replication_start",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_group_replication_start", 0, 0, false),
		},
		"Com_group_replication_stop": &sql.MySQLStatusCounter{
			Name:  "Com_group_replication_stop",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_group_replication_stop", 0, 0, false),
		},
		"Com_ha_close": &sql.MySQLStatusCounter{
			Name:  "Com_ha_close",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_ha_close", 0, 0, false),
		},
		"Com_ha_open": &sql.MySQLStatusCounter{
			Name:  "Com_ha_open",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_ha_open", 0, 0, false),
		},
		"Com_ha_read": &sql.MySQLStatusCounter{
			Name:  "Com_ha_read",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_ha_read", 0, 0, false),
		},
		"Com_help": &sql.MySQLStatusCounter{
			Name:  "Com_help",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_help", 0, 0, false),
		},
		"Com_insert": &sql.MySQLStatusCounter{
			Name:  "Com_insert",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_insert", 0, 0, false),
		},
		"Com_insert_select": &sql.MySQLStatusCounter{
			Name:  "Com_insert_select",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_insert_select", 0, 0, false),
		},
		"Com_install_component": &sql.MySQLStatusCounter{
			Name:  "Com_install_component",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_install_component", 0, 0, false),
		},
		"Com_install_plugin": &sql.MySQLStatusCounter{
			Name:  "Com_install_plugin",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_install_plugin", 0, 0, false),
		},
		"Com_kill": &sql.MySQLStatusCounter{
			Name:  "Com_kill",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_kill", 0, 0, false),
		},
		"Com_load": &sql.MySQLStatusCounter{
			Name:  "Com_load",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_load", 0, 0, false),
		},
		"Com_lock_tables": &sql.MySQLStatusCounter{
			Name:  "Com_lock_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_lock_tables", 0, 0, false),
		},
		"Com_optimize": &sql.MySQLStatusCounter{
			Name:  "Com_optimize",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_optimize", 0, 0, false),
		},
		"Com_preload_keys": &sql.MySQLStatusCounter{
			Name:  "Com_preload_keys",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_preload_keys", 0, 0, false),
		},
		"Com_prepare_sql": &sql.MySQLStatusCounter{
			Name:  "Com_prepare_sql",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_prepare_sql", 0, 0, false),
		},
		"Com_purge": &sql.MySQLStatusCounter{
			Name:  "Com_purge",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_purge", 0, 0, false),
		},
		"Com_purge_before_date": &sql.MySQLStatusCounter{
			Name:  "Com_purge_before_date",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_purge_before_date", 0, 0, false),
		},
		"Com_release_savepoint": &sql.MySQLStatusCounter{
			Name:  "Com_release_savepoint",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_release_savepoint", 0, 0, false),
		},
		"Com_rename_table": &sql.MySQLStatusCounter{
			Name:  "Com_rename_table",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_rename_table", 0, 0, false),
		},
		"Com_rename_user": &sql.MySQLStatusCounter{
			Name:  "Com_rename_user",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_rename_user", 0, 0, false),
		},
		"Com_repair": &sql.MySQLStatusCounter{
			Name:  "Com_repair",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_repair", 0, 0, false),
		},
		"Com_replace": &sql.MySQLStatusCounter{
			Name:  "Com_replace",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_replace", 0, 0, false),
		},
		"Com_replace_select": &sql.MySQLStatusCounter{
			Name:  "Com_replace_select",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_replace_select", 0, 0, false),
		},
		"Com_replica_start": &sql.MySQLStatusCounter{
			Name:  "Com_replica_start",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_replica_start", 0, 0, false),
		},
		"Com_replica_stop": &sql.MySQLStatusCounter{
			Name:  "Com_replica_stop",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_replica_stop", 0, 0, false),
		},
		"Com_reset": &sql.MySQLStatusCounter{
			Name:  "Com_reset",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_reset", 0, 0, false),
		},
		"Com_resignal": &sql.MySQLStatusCounter{
			Name:  "Com_resignal",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_resignal", 0, 0, false),
		},
		"Com_restart": &sql.MySQLStatusCounter{
			Name:  "Com_restart",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_restart", 0, 0, false),
		},
		"Com_revoke": &sql.MySQLStatusCounter{
			Name:  "Com_revoke",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_revoke", 0, 0, false),
		},
		"Com_revoke_all": &sql.MySQLStatusCounter{
			Name:  "Com_revoke_all",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_revoke_all", 0, 0, false),
		},
		"Com_revoke_roles": &sql.MySQLStatusCounter{
			Name:  "Com_revoke_roles",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_revoke_roles", 0, 0, false),
		},
		"Com_rollback": &sql.MySQLStatusCounter{
			Name:  "Com_rollback",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_rollback", 0, 0, false),
		},
		"Com_rollback_to_savepoint": &sql.MySQLStatusCounter{
			Name:  "Com_rollback_to_savepoint",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_rollback_to_savepoint", 0, 0, false),
		},
		"Com_savepoint": &sql.MySQLStatusCounter{
			Name:  "Com_savepoint",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_savepoint", 0, 0, false),
		},
		"Com_select": &sql.MySQLStatusCounter{
			Name:  "Com_select",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_select", 0, 0, false),
		},
		"Com_set_option": &sql.MySQLStatusCounter{
			Name:  "Com_set_option",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_set_option", 0, 0, false),
		},
		"Com_set_resource_group": &sql.MySQLStatusCounter{
			Name:  "Com_set_resource_group",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_set_resource_group", 0, 0, false),
		},
		"Com_set_role": &sql.MySQLStatusCounter{
			Name:  "Com_set_role",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_set_role", 0, 0, false),
		},
		"Com_show_authors": &sql.MySQLStatusCounter{
			Name:  "Com_show_authors",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_authors", 0, 0, false),
		},
		"Com_show_binlog_events": &sql.MySQLStatusCounter{
			Name:  "Com_show_binlog_events",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_binlog_events", 0, 0, false),
		},
		"Com_show_binlogs": &sql.MySQLStatusCounter{
			Name:  "Com_show_binlogs",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_binlogs", 0, 0, false),
		},
		"Com_show_charsets": &sql.MySQLStatusCounter{
			Name:  "Com_show_charsets",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_charsets", 0, 0, false),
		},
		"Com_show_collations": &sql.MySQLStatusCounter{
			Name:  "Com_show_collations",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_collations", 0, 0, false),
		},
		"Com_show_contributors": &sql.MySQLStatusCounter{
			Name:  "Com_show_contributors",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_contributors", 0, 0, false),
		},
		"Com_show_create_db": &sql.MySQLStatusCounter{
			Name:  "Com_show_create_db",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_create_db", 0, 0, false),
		},
		"Com_show_create_event": &sql.MySQLStatusCounter{
			Name:  "Com_show_create_event",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_create_event", 0, 0, false),
		},
		"Com_show_create_func": &sql.MySQLStatusCounter{
			Name:  "Com_show_create_func",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_create_func", 0, 0, false),
		},
		"Com_show_create_proc": &sql.MySQLStatusCounter{
			Name:  "Com_show_create_proc",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_create_proc", 0, 0, false),
		},
		"Com_show_create_table": &sql.MySQLStatusCounter{
			Name:  "Com_show_create_table",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_create_table", 0, 0, false),
		},
		"Com_show_create_trigger": &sql.MySQLStatusCounter{
			Name:  "Com_show_create_trigger",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_create_trigger", 0, 0, false),
		},
		"Com_show_create_user": &sql.MySQLStatusCounter{
			Name:  "Com_show_create_user",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_create_user", 0, 0, false),
		},
		"Com_show_databases": &sql.MySQLStatusCounter{
			Name:  "Com_show_databases",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_databases", 0, 0, false),
		},
		"Com_show_engine_logs": &sql.MySQLStatusCounter{
			Name:  "Com_show_engine_logs",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_engine_logs", 0, 0, false),
		},
		"Com_show_engine_mutex": &sql.MySQLStatusCounter{
			Name:  "Com_show_engine_mutex",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_engine_mutex", 0, 0, false),
		},
		"Com_show_engine_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_engine_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_engine_status", 0, 0, false),
		},
		"Com_show_errors": &sql.MySQLStatusCounter{
			Name:  "Com_show_errors",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_errors", 0, 0, false),
		},
		"Com_show_events": &sql.MySQLStatusCounter{
			Name:  "Com_show_events",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_events", 0, 0, false),
		},
		"Com_show_fields": &sql.MySQLStatusCounter{
			Name:  "Com_show_fields",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_fields", 0, 0, false),
		},
		"Com_show_function_code": &sql.MySQLStatusCounter{
			Name:  "Com_show_function_code",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_function_code", 0, 0, false),
		},
		"Com_show_function_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_function_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_function_status", 0, 0, false),
		},
		"Com_show_grants": &sql.MySQLStatusCounter{
			Name:  "Com_show_grants",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_grants", 0, 0, false),
		},
		"Com_show_keys": &sql.MySQLStatusCounter{
			Name:  "Com_show_keys",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_keys", 0, 0, false),
		},
		"Com_show_master_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_master_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_master_status", 0, 0, false),
		},
		"Com_show_ndb_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_ndb_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_ndb_status", 0, 0, false),
		},
		"Com_show_open_tables": &sql.MySQLStatusCounter{
			Name:  "Com_show_open_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_open_tables", 0, 0, false),
		},
		"Com_show_plugins": &sql.MySQLStatusCounter{
			Name:  "Com_show_plugins",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_plugins", 0, 0, false),
		},
		"Com_show_privileges": &sql.MySQLStatusCounter{
			Name:  "Com_show_privileges",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_privileges", 0, 0, false),
		},
		"Com_show_procedure_code": &sql.MySQLStatusCounter{
			Name:  "Com_show_procedure_code",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_procedure_code", 0, 0, false),
		},
		"Com_show_procedure_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_procedure_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_procedure_status", 0, 0, false),
		},
		"Com_show_processlist": &sql.MySQLStatusCounter{
			Name:  "Com_show_processlist",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_processlist", 0, 0, false),
		},
		"Com_show_profile": &sql.MySQLStatusCounter{
			Name:  "Com_show_profile",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_profile", 0, 0, false),
		},
		"Com_show_profiles": &sql.MySQLStatusCounter{
			Name:  "Com_show_profiles",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_profiles", 0, 0, false),
		},
		"Com_show_relaylog_events": &sql.MySQLStatusCounter{
			Name:  "Com_show_relaylog_events",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_relaylog_events", 0, 0, false),
		},
		"Com_show_replica_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_replica_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_replica_status", 0, 0, false),
		},
		"Com_show_replicas": &sql.MySQLStatusCounter{
			Name:  "Com_show_replicas",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_replicas", 0, 0, false),
		},
		"Com_show_slave_hosts": &sql.MySQLStatusCounter{
			Name:  "Com_show_slave_hosts",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_slave_hosts", 0, 0, false),
		},
		"Com_show_slave_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_slave_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_slave_status", 0, 0, false),
		},
		"Com_show_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_status", 0, 0, false),
		},
		"Com_show_storage_engines": &sql.MySQLStatusCounter{
			Name:  "Com_show_storage_engines",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_storage_engines", 0, 0, false),
		},
		"Com_show_table_status": &sql.MySQLStatusCounter{
			Name:  "Com_show_table_status",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_table_status", 0, 0, false),
		},
		"Com_show_tables": &sql.MySQLStatusCounter{
			Name:  "Com_show_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_tables", 0, 0, false),
		},
		"Com_show_triggers": &sql.MySQLStatusCounter{
			Name:  "Com_show_triggers",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_triggers", 0, 0, false),
		},
		"Com_show_variables": &sql.MySQLStatusCounter{
			Name:  "Com_show_variables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_variables", 0, 0, false),
		},
		"Com_show_warnings": &sql.MySQLStatusCounter{
			Name:  "Com_show_warnings",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_show_warnings", 0, 0, false),
		},
		"Com_shutdown": &sql.MySQLStatusCounter{
			Name:  "Com_shutdown",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_shutdown", 0, 0, false),
		},
		"Com_signal": &sql.MySQLStatusCounter{
			Name:  "Com_signal",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_signal", 0, 0, false),
		},
		"Com_slave_start": &sql.MySQLStatusCounter{
			Name:  "Com_slave_start",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_slave_start", 0, 0, false),
		},
		"Com_slave_stop": &sql.MySQLStatusCounter{
			Name:  "Com_slave_stop",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_slave_stop", 0, 0, false),
		},
		"Com_stmt_close": &sql.MySQLStatusCounter{
			Name:  "Com_stmt_close",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_stmt_close", 0, 0, false),
		},
		"Com_stmt_execute": &sql.MySQLStatusCounter{
			Name:  "Com_stmt_execute",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_stmt_execute", 0, 0, false),
		},
		"Com_stmt_fetch": &sql.MySQLStatusCounter{
			Name:  "Com_stmt_fetch",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_stmt_fetch", 0, 0, false),
		},
		"Com_stmt_prepare": &sql.MySQLStatusCounter{
			Name:  "Com_stmt_prepare",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_stmt_prepare", 0, 0, false),
		},
		"Com_stmt_reprepare": &sql.MySQLStatusCounter{
			Name:  "Com_stmt_reprepare",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_stmt_reprepare", 0, 0, false),
		},
		"Com_stmt_reset": &sql.MySQLStatusCounter{
			Name:  "Com_stmt_reset",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_stmt_reset", 0, 0, false),
		},
		"Com_stmt_send_long_data": &sql.MySQLStatusCounter{
			Name:  "Com_stmt_send_long_data",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_stmt_send_long_data", 0, 0, false),
		},
		"Com_truncate": &sql.MySQLStatusCounter{
			Name:  "Com_truncate",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_truncate", 0, 0, false),
		},
		"Com_uninstall_component": &sql.MySQLStatusCounter{
			Name:  "Com_uninstall_component",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Com_uninstall_component", 0, 0, false),
		},
		"Com_uninstall_plugin": &sql.MySQLStatusCounter{
			Name:  "Com_uninstall_plugin",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_uninstall_plugin", 0, 0, false),
		},
		"Com_unlock_tables": &sql.MySQLStatusCounter{
			Name:  "Com_unlock_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_unlock_tables", 0, 0, false),
		},
		"Com_update": &sql.MySQLStatusCounter{
			Name:  "Com_update",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_update", 0, 0, false),
		},
		"Com_update_multi": &sql.MySQLStatusCounter{
			Name:  "Com_update_multi",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_update_multi", 0, 0, false),
		},
		"Com_xa_commit": &sql.MySQLStatusCounter{
			Name:  "Com_xa_commit",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_xa_commit", 0, 0, false),
		},
		"Com_xa_end": &sql.MySQLStatusCounter{
			Name:  "Com_xa_end",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_xa_end", 0, 0, false),
		},
		"Com_xa_prepare": &sql.MySQLStatusCounter{
			Name:  "Com_xa_prepare",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_xa_prepare", 0, 0, false),
		},
		"Com_xa_recover": &sql.MySQLStatusCounter{
			Name:  "Com_xa_recover",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_xa_recover", 0, 0, false),
		},
		"Com_xa_rollback": &sql.MySQLStatusCounter{
			Name:  "Com_xa_rollback",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_xa_rollback", 0, 0, false),
		},
		"Com_xa_start": &sql.MySQLStatusCounter{
			Name:  "Com_xa_start",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Com_xa_start", 0, 0, false),
		},
		"Compression": &sql.MySQLStatusCounter{
			Name:  "Compression",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Compression", 0, 0, false),
		},
		"Compression_algorithm": &sql.MySQLStatusVariable{
			Name:    "Compression_algorithm",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Compression_algorithm"),
			Default: "",
		},
		"Compression_level": &sql.MySQLStatusCounter{
			Name:  "Compression_level",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Compression_level", 0, 0, false),
		},
		"Connection_control_delay_generated": &sql.MySQLStatusCounter{
			Name:  "Connection_control_delay_generated",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connection_control_delay_generated", 0, 0, false),
		},
		"Connection_errors_accept": &sql.MySQLStatusCounter{
			Name:  "Connection_errors_accept",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connection_errors_accept", 0, 0, false),
		},
		"Connection_errors_internal": &sql.MySQLStatusCounter{
			Name:  "Connection_errors_internal",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connection_errors_internal", 0, 0, false),
		},
		"Connection_errors_max_connections": &sql.MySQLStatusCounter{
			Name:  "Connection_errors_max_connections",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connection_errors_max_connections", 0, 0, false),
		},
		"Connection_errors_peer_address": &sql.MySQLStatusCounter{
			Name:  "Connection_errors_peer_address",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connection_errors_peer_address", 0, 0, false),
		},
		"Connection_errors_select": &sql.MySQLStatusCounter{
			Name:  "Connection_errors_select",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connection_errors_select", 0, 0, false),
		},
		"Connection_errors_tcpwrap": &sql.MySQLStatusCounter{
			Name:  "Connection_errors_tcpwrap",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connection_errors_tcpwrap", 0, 0, false),
		},
		"Connections": &sql.MySQLStatusCounter{
			Name:  "Connections",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Connections", 0, 0, false),
		},
		"Created_tmp_disk_tables": &sql.MySQLStatusCounter{
			Name:  "Created_tmp_disk_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Created_tmp_disk_tables", 0, 0, false),
		},
		"Created_tmp_files": &sql.MySQLStatusCounter{
			Name:  "Created_tmp_files",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Created_tmp_files", 0, 0, false),
		},
		"Created_tmp_tables": &sql.MySQLStatusCounter{
			Name:  "Created_tmp_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Created_tmp_tables", 0, 0, false),
		},
		"Current_tls_ca": &sql.MySQLStatusVariable{
			Name:    "Current_tls_ca",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_ca"),
			Default: "",
		},
		"Current_tls_capath": &sql.MySQLStatusVariable{
			Name:    "Current_tls_capath",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_capath"),
			Default: "",
		},
		"Current_tls_cert": &sql.MySQLStatusVariable{
			Name:    "Current_tls_cert",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_cert"),
			Default: "",
		},
		"Current_tls_cipher": &sql.MySQLStatusVariable{
			Name:    "Current_tls_cipher",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_cipher"),
			Default: "",
		},
		"Current_tls_ciphersuites": &sql.MySQLStatusVariable{
			Name:    "Current_tls_ciphersuites",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_ciphersuites"),
			Default: "",
		},
		"Current_tls_crl": &sql.MySQLStatusVariable{
			Name:    "Current_tls_crl",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_crl"),
			Default: "",
		},
		"Current_tls_crlpath": &sql.MySQLStatusVariable{
			Name:    "Current_tls_crlpath",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_crlpath"),
			Default: "",
		},
		"Current_tls_key": &sql.MySQLStatusVariable{
			Name:    "Current_tls_key",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_key"),
			Default: "",
		},
		"Current_tls_version": &sql.MySQLStatusVariable{
			Name:    "Current_tls_version",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Current_tls_version"),
			Default: "",
		},
		"Delayed_errors": &sql.MySQLStatusCounter{
			Name:  "Delayed_errors",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Delayed_errors", 0, 0, false),
		},
		"Delayed_insert_threads": &sql.MySQLStatusCounter{
			Name:  "Delayed_insert_threads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Delayed_insert_threads", 0, 0, false),
		},
		"Delayed_writes": &sql.MySQLStatusCounter{
			Name:  "Delayed_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Delayed_writes", 0, 0, false),
		},
		"dragnet.Status": &sql.MySQLStatusVariable{
			Name:    "dragnet.Status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("dragnet.Status"),
			Default: "",
		},
		"Error_log_buffered_bytes": &sql.MySQLStatusCounter{
			Name:  "Error_log_buffered_bytes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Error_log_buffered_bytes", 0, 0, false),
		},
		"Error_log_buffered_events": &sql.MySQLStatusCounter{
			Name:  "Error_log_buffered_events",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Error_log_buffered_events", 0, 0, false),
		},
		"Error_log_expired_events": &sql.MySQLStatusCounter{
			Name:  "Error_log_expired_events",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Error_log_expired_events", 0, 0, false),
		},
		"Error_log_latest_write": &sql.MySQLStatusCounter{
			Name:  "Error_log_latest_write",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Error_log_latest_write", 0, 0, false),
		},
		"Firewall_access_denied": &sql.MySQLStatusCounter{
			Name:  "Firewall_access_denied",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Firewall_access_denied", 0, 0, false),
		},
		"Firewall_access_granted": &sql.MySQLStatusCounter{
			Name:  "Firewall_access_granted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Firewall_access_granted", 0, 0, false),
		},
		"Firewall_cached_entries": &sql.MySQLStatusCounter{
			Name:  "Firewall_cached_entries",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Firewall_cached_entries", 0, 0, false),
		},
		"Flush_commands": &sql.MySQLStatusCounter{
			Name:  "Flush_commands",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Flush_commands", 0, 0, false),
		},
		"Global_connection_memory": &sql.MySQLStatusCounter{
			Name:  "Global_connection_memory",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Global_connection_memory", 0, 0, false),
		},
		"group_replication_primary_member": &sql.MySQLStatusVariable{
			Name:    "group_replication_primary_member",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("group_replication_primary_member"),
			Default: "",
		},
		"Handler_commit": &sql.MySQLStatusCounter{
			Name:  "Handler_commit",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_commit", 0, 0, false),
		},
		"Handler_delete": &sql.MySQLStatusCounter{
			Name:  "Handler_delete",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_delete", 0, 0, false),
		},
		"Handler_discover": &sql.MySQLStatusCounter{
			Name:  "Handler_discover",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_discover", 0, 0, false),
		},
		"Handler_external_lock": &sql.MySQLStatusCounter{
			Name:  "Handler_external_lock",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_external_lock", 0, 0, false),
		},
		"Handler_mrr_init": &sql.MySQLStatusCounter{
			Name:  "Handler_mrr_init",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_mrr_init", 0, 0, false),
		},
		"Handler_prepare": &sql.MySQLStatusCounter{
			Name:  "Handler_prepare",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_prepare", 0, 0, false),
		},
		"Handler_read_first": &sql.MySQLStatusCounter{
			Name:  "Handler_read_first",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_read_first", 0, 0, false),
		},
		"Handler_read_key": &sql.MySQLStatusCounter{
			Name:  "Handler_read_key",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_read_key", 0, 0, false),
		},
		"Handler_read_last": &sql.MySQLStatusCounter{
			Name:  "Handler_read_last",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_read_last", 0, 0, false),
		},
		"Handler_read_next": &sql.MySQLStatusCounter{
			Name:  "Handler_read_next",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_read_next", 0, 0, false),
		},
		"Handler_read_prev": &sql.MySQLStatusCounter{
			Name:  "Handler_read_prev",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_read_prev", 0, 0, false),
		},
		"Handler_read_rnd": &sql.MySQLStatusCounter{
			Name:  "Handler_read_rnd",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_read_rnd", 0, 0, false),
		},
		"Handler_read_rnd_next": &sql.MySQLStatusCounter{
			Name:  "Handler_read_rnd_next",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_read_rnd_next", 0, 0, false),
		},
		"Handler_rollback": &sql.MySQLStatusCounter{
			Name:  "Handler_rollback",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_rollback", 0, 0, false),
		},
		"Handler_savepoint": &sql.MySQLStatusCounter{
			Name:  "Handler_savepoint",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_savepoint", 0, 0, false),
		},
		"Handler_savepoint_rollback": &sql.MySQLStatusCounter{
			Name:  "Handler_savepoint_rollback",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_savepoint_rollback", 0, 0, false),
		},
		"Handler_update": &sql.MySQLStatusCounter{
			Name:  "Handler_update",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_update", 0, 0, false),
		},
		"Handler_write": &sql.MySQLStatusCounter{
			Name:  "Handler_write",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Handler_write", 0, 0, false),
		},
		"Innodb_buffer_pool_bytes_data": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_bytes_data",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_bytes_data", 0, 0, false),
		},
		"Innodb_buffer_pool_bytes_dirty": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_bytes_dirty",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_bytes_dirty", 0, 0, false),
		},
		"Innodb_buffer_pool_dump_status": &sql.MySQLStatusVariable{
			Name:    "Innodb_buffer_pool_dump_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Innodb_buffer_pool_dump_status"),
			Default: "",
		},
		"Innodb_buffer_pool_load_status": &sql.MySQLStatusVariable{
			Name:    "Innodb_buffer_pool_load_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Innodb_buffer_pool_load_status"),
			Default: "",
		},
		"Innodb_buffer_pool_pages_data": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_pages_data",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_pages_data", 0, 0, false),
		},
		"Innodb_buffer_pool_pages_dirty": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_pages_dirty",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_pages_dirty", 0, 0, false),
		},
		"Innodb_buffer_pool_pages_flushed": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_pages_flushed",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_pages_flushed", 0, 0, false),
		},
		"Innodb_buffer_pool_pages_free": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_pages_free",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_pages_free", 0, 0, false),
		},
		"Innodb_buffer_pool_pages_latched": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_pages_latched",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_pages_latched", 0, 0, false),
		},
		"Innodb_buffer_pool_pages_misc": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_pages_misc",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_pages_misc", 0, 0, false),
		},
		"Innodb_buffer_pool_pages_total": &sql.MySQLStatusVariable{
			Name:  "Innodb_buffer_pool_pages_total",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_pages_total", 0, 0, false),
			// NOTE: Datadog errors out with a divide by zero error if this is zero,
			//       so for now, we just report 1.
			Default: int64(1),
		},
		"Innodb_buffer_pool_read_ahead": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_read_ahead",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_read_ahead", 0, 0, false),
		},
		"Innodb_buffer_pool_read_ahead_evicted": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_read_ahead_evicted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_read_ahead_evicted", 0, 0, false),
		},
		"Innodb_buffer_pool_read_ahead_rnd": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_read_ahead_rnd",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_read_ahead_rnd", 0, 0, false),
		},
		"Innodb_buffer_pool_read_requests": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_read_requests",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_read_requests", 0, 0, false),
		},
		"Innodb_buffer_pool_reads": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_reads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_reads", 0, 0, false),
		},
		"Innodb_buffer_pool_resize_status": &sql.MySQLStatusVariable{
			Name:    "Innodb_buffer_pool_resize_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Innodb_buffer_pool_resize_status"),
			Default: "",
		},
		"Innodb_buffer_pool_resize_status_code": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_resize_status_code",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_resize_status_code", 0, 0, false),
		},
		"Innodb_buffer_pool_resize_status_progress": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_resize_status_progress",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_resize_status_progress", 0, 0, false),
		},
		"Innodb_buffer_pool_wait_free": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_wait_free",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_wait_free", 0, 0, false),
		},
		"Innodb_buffer_pool_write_requests": &sql.MySQLStatusCounter{
			Name:  "Innodb_buffer_pool_write_requests",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_buffer_pool_write_requests", 0, 0, false),
		},
		"Innodb_data_fsyncs": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_fsyncs",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_fsyncs", 0, 0, false),
		},
		"Innodb_data_pending_fsyncs": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_pending_fsyncs",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_pending_fsyncs", 0, 0, false),
		},
		"Innodb_data_pending_reads": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_pending_reads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_pending_reads", 0, 0, false),
		},
		"Innodb_data_pending_writes": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_pending_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_pending_writes", 0, 0, false),
		},
		"Innodb_data_read": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_read",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_read", 0, 0, false),
		},
		"Innodb_data_reads": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_reads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_reads", 0, 0, false),
		},
		"Innodb_data_writes": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_writes", 0, 0, false),
		},
		"Innodb_data_written": &sql.MySQLStatusCounter{
			Name:  "Innodb_data_written",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_data_written", 0, 0, false),
		},
		"Innodb_dblwr_pages_written": &sql.MySQLStatusCounter{
			Name:  "Innodb_dblwr_pages_written",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_dblwr_pages_written", 0, 0, false),
		},
		"Innodb_dblwr_writes": &sql.MySQLStatusCounter{
			Name:  "Innodb_dblwr_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_dblwr_writes", 0, 0, false),
		},
		"Innodb_have_atomic_builtins": &sql.MySQLStatusCounter{
			Name:  "Innodb_have_atomic_builtins",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_have_atomic_builtins", 0, 0, false),
		},
		"Innodb_log_waits": &sql.MySQLStatusCounter{
			Name:  "Innodb_log_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_log_waits", 0, 0, false),
		},
		"Innodb_log_write_requests": &sql.MySQLStatusCounter{
			Name:  "Innodb_log_write_requests",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_log_write_requests", 0, 0, false),
		},
		"Innodb_log_writes": &sql.MySQLStatusCounter{
			Name:  "Innodb_log_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_log_writes", 0, 0, false),
		},
		"Innodb_num_open_files": &sql.MySQLStatusCounter{
			Name:  "Innodb_num_open_files",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_num_open_files", 0, 0, false),
		},
		"Innodb_os_log_fsyncs": &sql.MySQLStatusCounter{
			Name:  "Innodb_os_log_fsyncs",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_os_log_fsyncs", 0, 0, false),
		},
		"Innodb_os_log_pending_fsyncs": &sql.MySQLStatusCounter{
			Name:  "Innodb_os_log_pending_fsyncs",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_os_log_pending_fsyncs", 0, 0, false),
		},
		"Innodb_os_log_pending_writes": &sql.MySQLStatusCounter{
			Name:  "Innodb_os_log_pending_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_os_log_pending_writes", 0, 0, false),
		},
		"Innodb_os_log_written": &sql.MySQLStatusCounter{
			Name:  "Innodb_os_log_written",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_os_log_written", 0, 0, false),
		},
		"Innodb_page_size": &sql.MySQLStatusCounter{
			Name:  "Innodb_page_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_page_size", 0, 0, false),
		},
		"Innodb_pages_created": &sql.MySQLStatusCounter{
			Name:  "Innodb_pages_created",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_pages_created", 0, 0, false),
		},
		"Innodb_pages_read": &sql.MySQLStatusCounter{
			Name:  "Innodb_pages_read",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_pages_read", 0, 0, false),
		},
		"Innodb_pages_written": &sql.MySQLStatusCounter{
			Name:  "Innodb_pages_written",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_pages_written", 0, 0, false),
		},
		"Innodb_redo_log_capacity_resized": &sql.MySQLStatusCounter{
			Name:  "Innodb_redo_log_capacity_resized",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_redo_log_capacity_resized", 0, 0, false),
		},
		"Innodb_redo_log_checkpoint_lsn": &sql.MySQLStatusCounter{
			Name:  "Innodb_redo_log_checkpoint_lsn",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_redo_log_checkpoint_lsn", 0, 0, false),
		},
		"Innodb_redo_log_current_lsn": &sql.MySQLStatusCounter{
			Name:  "Innodb_redo_log_current_lsn",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_redo_log_current_lsn", 0, 0, false),
		},
		"Innodb_redo_log_enabled": &sql.MySQLStatusVariable{
			Name:    "Innodb_redo_log_enabled",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Innodb_redo_log_enabled"),
			Default: int8(0),
		},
		"Innodb_redo_log_flushed_to_disk_lsn": &sql.MySQLStatusCounter{
			Name:  "Innodb_redo_log_flushed_to_disk_lsn",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_redo_log_flushed_to_disk_lsn", 0, 0, false),
		},
		"Innodb_redo_log_logical_size": &sql.MySQLStatusCounter{
			Name:  "Innodb_redo_log_logical_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_redo_log_logical_size", 0, 0, false),
		},
		"Innodb_redo_log_physical_size": &sql.MySQLStatusVariable{
			Name:    "Innodb_redo_log_physical_size",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Innodb_redo_log_physical_size"),
			Default: int8(0),
		},
		"Innodb_redo_log_read_only": &sql.MySQLStatusVariable{
			Name:    "Innodb_redo_log_read_only",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Innodb_redo_log_read_only"),
			Default: int8(0),
		},
		"Innodb_redo_log_resize_status": &sql.MySQLStatusVariable{
			Name:    "Innodb_redo_log_resize_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Innodb_redo_log_resize_status"),
			Default: "",
		},
		"Innodb_redo_log_uuid": &sql.MySQLStatusCounter{
			Name:  "Innodb_redo_log_uuid",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_redo_log_uuid", 0, 0, false),
		},
		"Innodb_row_lock_current_waits": &sql.MySQLStatusCounter{
			Name:  "Innodb_row_lock_current_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_row_lock_current_waits", 0, 0, false),
		},
		"Innodb_row_lock_time": &sql.MySQLStatusCounter{
			Name:  "Innodb_row_lock_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_row_lock_time", 0, 0, false),
		},
		"Innodb_row_lock_time_avg": &sql.MySQLStatusCounter{
			Name:  "Innodb_row_lock_time_avg",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_row_lock_time_avg", 0, 0, false),
		},
		"Innodb_row_lock_time_max": &sql.MySQLStatusCounter{
			Name:  "Innodb_row_lock_time_max",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_row_lock_time_max", 0, 0, false),
		},
		"Innodb_row_lock_waits": &sql.MySQLStatusCounter{
			Name:  "Innodb_row_lock_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_row_lock_waits", 0, 0, false),
		},
		"Innodb_rows_deleted": &sql.MySQLStatusCounter{
			Name:  "Innodb_rows_deleted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_rows_deleted", 0, 0, false),
		},
		"Innodb_rows_inserted": &sql.MySQLStatusCounter{
			Name:  "Innodb_rows_inserted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_rows_inserted", 0, 0, false),
		},
		"Innodb_rows_read": &sql.MySQLStatusCounter{
			Name:  "Innodb_rows_read",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_rows_read", 0, 0, false),
		},
		"Innodb_rows_updated": &sql.MySQLStatusCounter{
			Name:  "Innodb_rows_updated",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_rows_updated", 0, 0, false),
		},
		"Innodb_system_rows_deleted": &sql.MySQLStatusCounter{
			Name:  "Innodb_system_rows_deleted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_system_rows_deleted", 0, 0, false),
		},
		"Innodb_system_rows_inserted": &sql.MySQLStatusCounter{
			Name:  "Innodb_system_rows_inserted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_system_rows_inserted", 0, 0, false),
		},
		"Innodb_system_rows_read": &sql.MySQLStatusCounter{
			Name:  "Innodb_system_rows_read",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_system_rows_read", 0, 0, false),
		},
		"Innodb_system_rows_updated": &sql.MySQLStatusCounter{
			Name:  "Innodb_system_rows_updated",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_system_rows_updated", 0, 0, false),
		},
		"Innodb_truncated_status_writes": &sql.MySQLStatusCounter{
			Name:  "Innodb_truncated_status_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_truncated_status_writes", 0, 0, false),
		},
		"Innodb_undo_tablespaces_active": &sql.MySQLStatusCounter{
			Name:  "Innodb_undo_tablespaces_active",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_undo_tablespaces_active", 0, 0, false),
		},
		"Innodb_undo_tablespaces_explicit": &sql.MySQLStatusCounter{
			Name:  "Innodb_undo_tablespaces_explicit",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_undo_tablespaces_explicit", 0, 0, false),
		},
		"Innodb_undo_tablespaces_implicit": &sql.MySQLStatusCounter{
			Name:  "Innodb_undo_tablespaces_implicit",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_undo_tablespaces_implicit", 0, 0, false),
		},
		"Innodb_undo_tablespaces_total": &sql.MySQLStatusCounter{
			Name:  "Innodb_undo_tablespaces_total",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Innodb_undo_tablespaces_total", 0, 0, false),
		},
		"Key_blocks_not_flushed": &sql.MySQLStatusCounter{
			Name:  "Key_blocks_not_flushed",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Key_blocks_not_flushed", 0, 0, false),
		},
		"Key_blocks_unused": &sql.MySQLStatusCounter{
			Name:  "Key_blocks_unused",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Key_blocks_unused", 0, 0, false),
		},
		"Key_blocks_used": &sql.MySQLStatusCounter{
			Name:  "Key_blocks_used",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Key_blocks_used", 0, 0, false),
		},
		"Key_read_requests": &sql.MySQLStatusCounter{
			Name:  "Key_read_requests",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Key_read_requests", 0, 0, false),
		},
		"Key_reads": &sql.MySQLStatusCounter{
			Name:  "Key_reads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Key_reads", 0, 0, false),
		},
		"Key_write_requests": &sql.MySQLStatusCounter{
			Name:  "Key_write_requests",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Key_write_requests", 0, 0, false),
		},
		"Key_writes": &sql.MySQLStatusCounter{
			Name:  "Key_writes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Key_writes", 0, 0, false),
		},
		"Last_query_cost": &sql.MySQLStatusCounter{
			Name:  "Last_query_cost",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Last_query_cost", 0, 0, false),
		},
		"Last_query_partial_plans": &sql.MySQLStatusCounter{
			Name:  "Last_query_partial_plans",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Last_query_partial_plans", 0, 0, false),
		},
		"Locked_connects": &sql.MySQLStatusCounter{
			Name:  "Locked_connects",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Locked_connects", 0, 0, false),
		},
		"Max_execution_time_exceeded": &sql.MySQLStatusCounter{
			Name:  "Max_execution_time_exceeded",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Max_execution_time_exceeded", 0, 0, false),
		},
		"Max_execution_time_set": &sql.MySQLStatusCounter{
			Name:  "Max_execution_time_set",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Max_execution_time_set", 0, 0, false),
		},
		"Max_execution_time_set_failed": &sql.MySQLStatusCounter{
			Name:  "Max_execution_time_set_failed",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Max_execution_time_set_failed", 0, 0, false),
		},
		"Max_used_connections": &sql.MySQLStatusCounter{
			Name:  "Max_used_connections",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Max_used_connections", 0, 0, false),
		},
		"Max_used_connections_time": &sql.MySQLStatusVariable{
			Name:    "Max_used_connections_time",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Max_used_connections_time"),
			Default: "",
		},
		"mecab_charset": &sql.MySQLStatusVariable{
			Name:    "mecab_charset",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("mecab_charset"),
			Default: "",
		},
		"Mysqlx_aborted_clients": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_aborted_clients",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_aborted_clients", 0, 0, false),
		},
		"Mysqlx_address": &sql.MySQLStatusVariable{
			Name:    "Mysqlx_address",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Mysqlx_address"),
			Default: "",
		},
		"Mysqlx_bytes_received": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_bytes_received",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_bytes_received", 0, 0, false),
		},
		"Mysqlx_bytes_received_compressed_payload": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_bytes_received_compressed_payload",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_bytes_received_compressed_payload", 0, 0, false),
		},
		"Mysqlx_bytes_received_uncompressed_frame": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_bytes_received_uncompressed_frame",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_bytes_received_uncompressed_frame", 0, 0, false),
		},
		"Mysqlx_bytes_sent": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_bytes_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_bytes_sent", 0, 0, false),
		},
		"Mysqlx_bytes_sent_compressed_payload": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_bytes_sent_compressed_payload",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_bytes_sent_compressed_payload", 0, 0, false),
		},
		"Mysqlx_bytes_sent_uncompressed_frame": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_bytes_sent_uncompressed_frame",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_bytes_sent_uncompressed_frame", 0, 0, false),
		},
		"Mysqlx_compression_algorithm": &sql.MySQLStatusVariable{
			Name:    "Mysqlx_compression_algorithm",
			Scope:   sql.StatusVariableScope_Session,
			Type:    types.NewSystemStringType("Mysqlx_compression_algorithm"),
			Default: "",
		},
		"Mysqlx_compression_level": &sql.MySQLStatusVariable{
			Name:    "Mysqlx_compression_level",
			Scope:   sql.StatusVariableScope_Session,
			Type:    types.NewSystemStringType("Mysqlx_compression_level"),
			Default: "",
		},
		"Mysqlx_connection_accept_errors": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_connection_accept_errors",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_connection_accept_errors", 0, 0, false),
		},
		"Mysqlx_connection_errors": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_connection_errors",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_connection_errors", 0, 0, false),
		},
		"Mysqlx_connections_accepted": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_connections_accepted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_connections_accepted", 0, 0, false),
		},
		"Mysqlx_connections_closed": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_connections_closed",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_connections_closed", 0, 0, false),
		},
		"Mysqlx_connections_rejected": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_connections_rejected",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_connections_rejected", 0, 0, false),
		},
		"Mysqlx_crud_create_view": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_crud_create_view",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_crud_create_view", 0, 0, false),
		},
		"Mysqlx_crud_delete": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_crud_delete",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_crud_delete", 0, 0, false),
		},
		"Mysqlx_crud_drop_view": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_crud_drop_view",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_crud_drop_view", 0, 0, false),
		},
		"Mysqlx_crud_find": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_crud_find",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_crud_find", 0, 0, false),
		},
		"Mysqlx_crud_insert": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_crud_insert",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_crud_insert", 0, 0, false),
		},
		"Mysqlx_crud_modify_view": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_crud_modify_view",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_crud_modify_view", 0, 0, false),
		},
		"Mysqlx_crud_update": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_crud_update",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_crud_update", 0, 0, false),
		},
		"Mysqlx_cursor_close": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_cursor_close",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_cursor_close", 0, 0, false),
		},
		"Mysqlx_cursor_fetch": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_cursor_fetch",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_cursor_fetch", 0, 0, false),
		},
		"Mysqlx_cursor_open": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_cursor_open",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_cursor_open", 0, 0, false),
		},
		"Mysqlx_errors_sent": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_errors_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_errors_sent", 0, 0, false),
		},
		"Mysqlx_errors_unknown_message_type": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_errors_unknown_message_type",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_errors_unknown_message_type", 0, 0, false),
		},
		"Mysqlx_expect_close": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_expect_close",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_expect_close", 0, 0, false),
		},
		"Mysqlx_expect_open": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_expect_open",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_expect_open", 0, 0, false),
		},
		"Mysqlx_init_error": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_init_error",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_init_error", 0, 0, false),
		},
		"Mysqlx_messages_sent": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_messages_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_messages_sent", 0, 0, false),
		},
		"Mysqlx_notice_global_sent": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_notice_global_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_notice_global_sent", 0, 0, false),
		},
		"Mysqlx_notice_other_sent": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_notice_other_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_notice_other_sent", 0, 0, false),
		},
		"Mysqlx_notice_warning_sent": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_notice_warning_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_notice_warning_sent", 0, 0, false),
		},
		"Mysqlx_notified_by_group_replication": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_notified_by_group_replication",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_notified_by_group_replication", 0, 0, false),
		},
		"Mysqlx_port": &sql.MySQLStatusVariable{
			Name:    "Mysqlx_port",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Mysqlx_port"),
			Default: "",
		},
		"Mysqlx_prep_deallocate": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_prep_deallocate",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_prep_deallocate", 0, 0, false),
		},
		"Mysqlx_prep_execute": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_prep_execute",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_prep_execute", 0, 0, false),
		},
		"Mysqlx_prep_prepare": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_prep_prepare",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_prep_prepare", 0, 0, false),
		},
		"Mysqlx_rows_sent": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_rows_sent",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_rows_sent", 0, 0, false),
		},
		"Mysqlx_sessions": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_sessions",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_sessions", 0, 0, false),
		},
		"Mysqlx_sessions_accepted": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_sessions_accepted",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_sessions_accepted", 0, 0, false),
		},
		"Mysqlx_sessions_closed": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_sessions_closed",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_sessions_closed", 0, 0, false),
		},
		"Mysqlx_sessions_fatal_error": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_sessions_fatal_error",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_sessions_fatal_error", 0, 0, false),
		},
		"Mysqlx_sessions_killed": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_sessions_killed",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_sessions_killed", 0, 0, false),
		},
		"Mysqlx_sessions_rejected": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_sessions_rejected",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_sessions_rejected", 0, 0, false),
		},
		"Mysqlx_socket": &sql.MySQLStatusVariable{
			Name:    "Mysqlx_socket",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Mysqlx_socket"),
			Default: "",
		},
		"Mysqlx_ssl_accept_renegotiates": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_accept_renegotiates",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_ssl_accept_renegotiates", 0, 0, false),
		},
		"Mysqlx_ssl_accepts": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_accepts",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_ssl_accepts", 0, 0, false),
		},
		"Mysqlx_ssl_active": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_active",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_ssl_active", 0, 0, false),
		},
		"Mysqlx_ssl_cipher": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_cipher",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_ssl_cipher", 0, 0, false),
		},
		"Mysqlx_ssl_cipher_list": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_cipher_list",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_ssl_cipher_list", 0, 0, false),
		},
		"Mysqlx_ssl_ctx_verify_depth": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_ctx_verify_depth",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_ssl_ctx_verify_depth", 0, 0, false),
		},
		"Mysqlx_ssl_ctx_verify_mode": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_ctx_verify_mode",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_ssl_ctx_verify_mode", 0, 0, false),
		},
		"Mysqlx_ssl_finished_accepts": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_finished_accepts",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_ssl_finished_accepts", 0, 0, false),
		},
		"Mysqlx_ssl_server_not_after": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_server_not_after",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_ssl_server_not_after", 0, 0, false),
		},
		"Mysqlx_ssl_server_not_before": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_server_not_before",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_ssl_server_not_before", 0, 0, false),
		},
		"Mysqlx_ssl_verify_depth": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_verify_depth",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_ssl_verify_depth", 0, 0, false),
		},
		"Mysqlx_ssl_verify_mode": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_verify_mode",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_ssl_verify_mode", 0, 0, false),
		},
		"Mysqlx_ssl_version": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_ssl_version",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_ssl_version", 0, 0, false),
		},
		"Mysqlx_stmt_create_collection": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_create_collection",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_create_collection", 0, 0, false),
		},
		"Mysqlx_stmt_create_collection_index": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_create_collection_index",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_create_collection_index", 0, 0, false),
		},
		"Mysqlx_stmt_disable_notices": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_disable_notices",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_disable_notices", 0, 0, false),
		},
		"Mysqlx_stmt_drop_collection": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_drop_collection",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_drop_collection", 0, 0, false),
		},
		"Mysqlx_stmt_drop_collection_index": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_drop_collection_index",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_drop_collection_index", 0, 0, false),
		},
		"Mysqlx_stmt_enable_notices": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_enable_notices",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_enable_notices", 0, 0, false),
		},
		"Mysqlx_stmt_ensure_collection": &sql.MySQLStatusVariable{
			Name:    "Mysqlx_stmt_ensure_collection",
			Scope:   sql.StatusVariableScope_Both,
			Type:    types.NewSystemStringType("Mysqlx_stmt_ensure_collection"),
			Default: "",
		},
		"Mysqlx_stmt_execute_mysqlx": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_execute_mysqlx",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_execute_mysqlx", 0, 0, false),
		},
		"Mysqlx_stmt_execute_sql": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_execute_sql",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_execute_sql", 0, 0, false),
		},
		"Mysqlx_stmt_execute_xplugin": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_execute_xplugin",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_execute_xplugin", 0, 0, false),
		},
		"Mysqlx_stmt_get_collection_options": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_get_collection_options",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_get_collection_options", 0, 0, false),
		},
		"Mysqlx_stmt_kill_client": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_kill_client",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_kill_client", 0, 0, false),
		},
		"Mysqlx_stmt_list_clients": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_list_clients",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_list_clients", 0, 0, false),
		},
		"Mysqlx_stmt_list_notices": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_list_notices",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_list_notices", 0, 0, false),
		},
		"Mysqlx_stmt_list_objects": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_list_objects",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_list_objects", 0, 0, false),
		},
		"Mysqlx_stmt_modify_collection_options": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_modify_collection_options",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_modify_collection_options", 0, 0, false),
		},
		"Mysqlx_stmt_ping": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_stmt_ping",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Mysqlx_stmt_ping", 0, 0, false),
		},
		"Mysqlx_worker_threads": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_worker_threads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_worker_threads", 0, 0, false),
		},
		"Mysqlx_worker_threads_active": &sql.MySQLStatusCounter{
			Name:  "Mysqlx_worker_threads_active",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Mysqlx_worker_threads_active", 0, 0, false),
		},
		"Ndb_api_adaptive_send_deferred_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_deferred_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_deferred_count", 0, 0, false),
		},
		"Ndb_api_adaptive_send_deferred_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_deferred_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_deferred_count_replica", 0, 0, false),
		},
		"Ndb_api_adaptive_send_deferred_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_deferred_count_session",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_deferred_count_session", 0, 0, false),
		},
		"Ndb_api_adaptive_send_deferred_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_deferred_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_deferred_count_slave", 0, 0, false),
		},
		"Ndb_api_adaptive_send_forced_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_forced_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_forced_count", 0, 0, false),
		},
		"Ndb_api_adaptive_send_forced_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_forced_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_forced_count_replica", 0, 0, false),
		},
		"Ndb_api_adaptive_send_forced_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_forced_count_session",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_forced_count_session", 0, 0, false),
		},
		"Ndb_api_adaptive_send_forced_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_forced_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_forced_count_slave", 0, 0, false),
		},
		"Ndb_api_adaptive_send_unforced_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_unforced_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_unforced_count", 0, 0, false),
		},
		"Ndb_api_adaptive_send_unforced_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_unforced_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_unforced_count_replica", 0, 0, false),
		},
		"Ndb_api_adaptive_send_unforced_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_unforced_count_session",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_unforced_count_session", 0, 0, false),
		},
		"Ndb_api_adaptive_send_unforced_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_adaptive_send_unforced_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_adaptive_send_unforced_count_slave", 0, 0, false),
		},
		"Ndb_api_bytes_received_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_received_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_bytes_received_count", 0, 0, false),
		},
		"Ndb_api_bytes_received_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_received_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_bytes_received_count_replica", 0, 0, false),
		},
		"Ndb_api_bytes_received_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_received_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_bytes_received_count_session", 0, 0, false),
		},
		"Ndb_api_bytes_received_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_received_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_bytes_received_count_slave", 0, 0, false),
		},
		"Ndb_api_bytes_sent_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_sent_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_bytes_sent_count", 0, 0, false),
		},
		"Ndb_api_bytes_sent_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_sent_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_bytes_sent_count_replica", 0, 0, false),
		},
		"Ndb_api_bytes_sent_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_sent_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_bytes_sent_count_session", 0, 0, false),
		},
		"Ndb_api_bytes_sent_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_bytes_sent_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_bytes_sent_count_slave", 0, 0, false),
		},
		"Ndb_api_event_bytes_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_event_bytes_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_event_bytes_count", 0, 0, false),
		},
		"Ndb_api_event_bytes_count_injector": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_event_bytes_count_injector",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_event_bytes_count_injector", 0, 0, false),
		},
		"Ndb_api_event_data_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_event_data_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_event_data_count", 0, 0, false),
		},
		"Ndb_api_event_data_count_injector": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_event_data_count_injector",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_event_data_count_injector", 0, 0, false),
		},
		"Ndb_api_event_nondata_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_event_nondata_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_event_nondata_count", 0, 0, false),
		},
		"Ndb_api_event_nondata_count_injector": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_event_nondata_count_injector",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_event_nondata_count_injector", 0, 0, false),
		},
		"Ndb_api_pk_op_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pk_op_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_pk_op_count", 0, 0, false),
		},
		"Ndb_api_pk_op_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pk_op_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_pk_op_count_replica", 0, 0, false),
		},
		"Ndb_api_pk_op_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pk_op_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_pk_op_count_session", 0, 0, false),
		},
		"Ndb_api_pk_op_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pk_op_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_pk_op_count_slave", 0, 0, false),
		},
		"Ndb_api_pruned_scan_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pruned_scan_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_pruned_scan_count", 0, 0, false),
		},
		"Ndb_api_pruned_scan_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pruned_scan_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_pruned_scan_count_replica", 0, 0, false),
		},
		"Ndb_api_pruned_scan_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pruned_scan_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_pruned_scan_count_session", 0, 0, false),
		},
		"Ndb_api_pruned_scan_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_pruned_scan_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_pruned_scan_count_slave", 0, 0, false),
		},
		"Ndb_api_range_scan_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_range_scan_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_range_scan_count", 0, 0, false),
		},
		"Ndb_api_range_scan_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_range_scan_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_range_scan_count_replica", 0, 0, false),
		},
		"Ndb_api_range_scan_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_range_scan_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_range_scan_count_session", 0, 0, false),
		},
		"Ndb_api_range_scan_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_range_scan_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_range_scan_count_slave", 0, 0, false),
		},
		"Ndb_api_read_row_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_read_row_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_read_row_count", 0, 0, false),
		},
		"Ndb_api_read_row_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_read_row_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_read_row_count_replica", 0, 0, false),
		},
		"Ndb_api_read_row_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_read_row_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_read_row_count_session", 0, 0, false),
		},
		"Ndb_api_read_row_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_read_row_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_read_row_count_slave", 0, 0, false),
		},
		"Ndb_api_scan_batch_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_scan_batch_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_scan_batch_count", 0, 0, false),
		},
		"Ndb_api_scan_batch_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_scan_batch_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_scan_batch_count_replica", 0, 0, false),
		},
		"Ndb_api_scan_batch_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_scan_batch_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_scan_batch_count_session", 0, 0, false),
		},
		"Ndb_api_scan_batch_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_scan_batch_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_scan_batch_count_slave", 0, 0, false),
		},
		"Ndb_api_table_scan_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_table_scan_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_table_scan_count", 0, 0, false),
		},
		"Ndb_api_table_scan_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_table_scan_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_table_scan_count_replica", 0, 0, false),
		},
		"Ndb_api_table_scan_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_table_scan_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_table_scan_count_session", 0, 0, false),
		},
		"Ndb_api_table_scan_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_table_scan_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_table_scan_count_slave", 0, 0, false),
		},
		"Ndb_api_trans_abort_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_abort_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_abort_count", 0, 0, false),
		},
		"Ndb_api_trans_abort_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_abort_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_abort_count_replica", 0, 0, false),
		},
		"Ndb_api_trans_abort_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_abort_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_trans_abort_count_session", 0, 0, false),
		},
		"Ndb_api_trans_abort_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_abort_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_abort_count_slave", 0, 0, false),
		},
		"Ndb_api_trans_close_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_close_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_close_count", 0, 0, false),
		},
		"Ndb_api_trans_close_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_close_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_close_count_replica", 0, 0, false),
		},
		"Ndb_api_trans_close_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_close_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_trans_close_count_session", 0, 0, false),
		},
		"Ndb_api_trans_close_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_close_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_close_count_slave", 0, 0, false),
		},
		"Ndb_api_trans_commit_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_commit_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_commit_count", 0, 0, false),
		},
		"Ndb_api_trans_commit_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_commit_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_commit_count_replica", 0, 0, false),
		},
		"Ndb_api_trans_commit_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_commit_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_trans_commit_count_session", 0, 0, false),
		},
		"Ndb_api_trans_commit_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_commit_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_commit_count_slave", 0, 0, false),
		},
		"Ndb_api_trans_local_read_row_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_local_read_row_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_local_read_row_count", 0, 0, false),
		},
		"Ndb_api_trans_local_read_row_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_local_read_row_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_local_read_row_count_replica", 0, 0, false),
		},
		"Ndb_api_trans_local_read_row_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_local_read_row_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_trans_local_read_row_count_session", 0, 0, false),
		},
		"Ndb_api_trans_local_read_row_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_local_read_row_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_local_read_row_count_slave", 0, 0, false),
		},
		"Ndb_api_trans_start_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_start_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_start_count", 0, 0, false),
		},
		"Ndb_api_trans_start_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_start_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_start_count_replica", 0, 0, false),
		},
		"Ndb_api_trans_start_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_start_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_trans_start_count_session", 0, 0, false),
		},
		"Ndb_api_trans_start_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_trans_start_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_trans_start_count_slave", 0, 0, false),
		},
		"Ndb_api_uk_op_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_uk_op_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_uk_op_count", 0, 0, false),
		},
		"Ndb_api_uk_op_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_uk_op_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_uk_op_count_replica", 0, 0, false),
		},
		"Ndb_api_uk_op_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_uk_op_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_uk_op_count_session", 0, 0, false),
		},
		"Ndb_api_uk_op_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_uk_op_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_uk_op_count_slave", 0, 0, false),
		},
		"Ndb_api_wait_exec_complete_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_exec_complete_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_exec_complete_count", 0, 0, false),
		},
		"Ndb_api_wait_exec_complete_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_exec_complete_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_exec_complete_count_replica", 0, 0, false),
		},
		"Ndb_api_wait_exec_complete_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_exec_complete_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_wait_exec_complete_count_session", 0, 0, false),
		},
		"Ndb_api_wait_exec_complete_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_exec_complete_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_exec_complete_count_slave", 0, 0, false),
		},
		"Ndb_api_wait_meta_request_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_meta_request_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_meta_request_count", 0, 0, false),
		},
		"Ndb_api_wait_meta_request_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_meta_request_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_meta_request_count_replica", 0, 0, false),
		},
		"Ndb_api_wait_meta_request_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_meta_request_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_wait_meta_request_count_session", 0, 0, false),
		},
		"Ndb_api_wait_meta_request_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_meta_request_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_meta_request_count_slave", 0, 0, false),
		},
		"Ndb_api_wait_nanos_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_nanos_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_nanos_count", 0, 0, false),
		},
		"Ndb_api_wait_nanos_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_nanos_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_nanos_count_replica", 0, 0, false),
		},
		"Ndb_api_wait_nanos_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_nanos_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_wait_nanos_count_session", 0, 0, false),
		},
		"Ndb_api_wait_nanos_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_nanos_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_nanos_count_slave", 0, 0, false),
		},
		"Ndb_api_wait_scan_result_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_scan_result_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_scan_result_count", 0, 0, false),
		},
		"Ndb_api_wait_scan_result_count_replica": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_scan_result_count_replica",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_scan_result_count_replica", 0, 0, false),
		},
		"Ndb_api_wait_scan_result_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_scan_result_count_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_api_wait_scan_result_count_session", 0, 0, false),
		},
		"Ndb_api_wait_scan_result_count_slave": &sql.MySQLStatusCounter{
			Name:  "Ndb_api_wait_scan_result_count_slave",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_api_wait_scan_result_count_slave", 0, 0, false),
		},
		"Ndb_cluster_node_id": &sql.MySQLStatusCounter{
			Name:  "Ndb_cluster_node_id",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_cluster_node_id", 0, 0, false),
		},
		"Ndb_config_from_host": &sql.MySQLStatusCounter{
			Name:  "Ndb_config_from_host",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ndb_config_from_host", 0, 0, false),
		},
		"Ndb_config_from_port": &sql.MySQLStatusCounter{
			Name:  "Ndb_config_from_port",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ndb_config_from_port", 0, 0, false),
		},
		"Ndb_config_generation": &sql.MySQLStatusCounter{
			Name:  "Ndb_config_generation",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_config_generation", 0, 0, false),
		},
		"Ndb_conflict_fn_epoch": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_epoch",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_epoch", 0, 0, false),
		},
		"Ndb_conflict_fn_epoch_trans": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_epoch_trans",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_epoch_trans", 0, 0, false),
		},
		"Ndb_conflict_fn_epoch2": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_epoch2",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_epoch2", 0, 0, false),
		},
		"Ndb_conflict_fn_epoch2_trans": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_epoch2_trans",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_epoch2_trans", 0, 0, false),
		},
		"Ndb_conflict_fn_max": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_max",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_max", 0, 0, false),
		},
		"Ndb_conflict_fn_max_del_win": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_max_del_win",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_max_del_win", 0, 0, false),
		},
		"Ndb_conflict_fn_max_del_win_ins": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_max_del_win_ins",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_max_del_win_ins", 0, 0, false),
		},
		"Ndb_conflict_fn_max_ins": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_max_ins",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_max_ins", 0, 0, false),
		},
		"Ndb_conflict_fn_old": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_fn_old",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_fn_old", 0, 0, false),
		},
		"Ndb_conflict_last_conflict_epoch": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_last_conflict_epoch",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_last_conflict_epoch", 0, 0, false),
		},
		"Ndb_conflict_last_stable_epoch": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_last_stable_epoch",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_last_stable_epoch", 0, 0, false),
		},
		"Ndb_conflict_reflected_op_discard_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_reflected_op_discard_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_reflected_op_discard_count", 0, 0, false),
		},
		"Ndb_conflict_reflected_op_prepare_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_reflected_op_prepare_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_reflected_op_prepare_count", 0, 0, false),
		},
		"Ndb_conflict_refresh_op_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_refresh_op_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_refresh_op_count", 0, 0, false),
		},
		"Ndb_conflict_trans_conflict_commit_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_trans_conflict_commit_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_trans_conflict_commit_count", 0, 0, false),
		},
		"Ndb_conflict_trans_detect_iter_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_trans_detect_iter_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_trans_detect_iter_count", 0, 0, false),
		},
		"Ndb_conflict_trans_reject_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_trans_reject_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_trans_reject_count", 0, 0, false),
		},
		"Ndb_conflict_trans_row_conflict_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_trans_row_conflict_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_trans_row_conflict_count", 0, 0, false),
		},
		"Ndb_conflict_trans_row_reject_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_conflict_trans_row_reject_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_conflict_trans_row_reject_count", 0, 0, false),
		},
		"Ndb_epoch_delete_delete_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_epoch_delete_delete_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_epoch_delete_delete_count", 0, 0, false),
		},
		"Ndb_execute_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_execute_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_execute_count", 0, 0, false),
		},
		"Ndb_last_commit_epoch_server": &sql.MySQLStatusCounter{
			Name:  "Ndb_last_commit_epoch_server",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_last_commit_epoch_server", 0, 0, false),
		},
		"Ndb_last_commit_epoch_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_last_commit_epoch_session",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ndb_last_commit_epoch_session", 0, 0, false),
		},
		"Ndb_metadata_blacklist_size": &sql.MySQLStatusCounter{
			Name:  "Ndb_metadata_blacklist_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_metadata_blacklist_size", 0, 0, false),
		},
		"Ndb_metadata_detected_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_metadata_detected_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_metadata_detected_count", 0, 0, false),
		},
		"Ndb_metadata_excluded_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_metadata_excluded_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_metadata_excluded_count", 0, 0, false),
		},
		"Ndb_metadata_synced_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_metadata_synced_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_metadata_synced_count", 0, 0, false),
		},
		"Ndb_number_of_data_nodes": &sql.MySQLStatusCounter{
			Name:  "Ndb_number_of_data_nodes",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_number_of_data_nodes", 0, 0, false),
		},
		"Ndb_pruned_scan_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_pruned_scan_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_pruned_scan_count", 0, 0, false),
		},
		"Ndb_pushed_queries_defined": &sql.MySQLStatusCounter{
			Name:  "Ndb_pushed_queries_defined",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_pushed_queries_defined", 0, 0, false),
		},
		"Ndb_pushed_queries_dropped": &sql.MySQLStatusCounter{
			Name:  "Ndb_pushed_queries_dropped",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_pushed_queries_dropped", 0, 0, false),
		},
		"Ndb_pushed_queries_executed": &sql.MySQLStatusCounter{
			Name:  "Ndb_pushed_queries_executed",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_pushed_queries_executed", 0, 0, false),
		},
		"Ndb_pushed_reads": &sql.MySQLStatusCounter{
			Name:  "Ndb_pushed_reads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_pushed_reads", 0, 0, false),
		},
		"Ndb_scan_count": &sql.MySQLStatusCounter{
			Name:  "Ndb_scan_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_scan_count", 0, 0, false),
		},
		"Ndb_slave_max_replicated_epoch": &sql.MySQLStatusCounter{
			Name:  "Ndb_slave_max_replicated_epoch",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ndb_slave_max_replicated_epoch", 0, 0, false),
		},
		"Ndb_trans_hint_count_session": &sql.MySQLStatusCounter{
			Name:  "Ndb_trans_hint_count_session",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ndb_trans_hint_count_session", 0, 0, false),
		},
		"Not_flushed_delayed_rows": &sql.MySQLStatusCounter{
			Name:  "Not_flushed_delayed_rows",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Not_flushed_delayed_rows", 0, 0, false),
		},
		"Ongoing_anonymous_gtid_violating_transaction_count": &sql.MySQLStatusCounter{
			Name:  "Ongoing_anonymous_gtid_violating_transaction_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ongoing_anonymous_gtid_violating_transaction_count", 0, 0, false),
		},
		"Ongoing_anonymous_transaction_count": &sql.MySQLStatusCounter{
			Name:  "Ongoing_anonymous_transaction_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ongoing_anonymous_transaction_count", 0, 0, false),
		},
		"Ongoing_automatic_gtid_violating_transaction_count": &sql.MySQLStatusCounter{
			Name:  "Ongoing_automatic_gtid_violating_transaction_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ongoing_automatic_gtid_violating_transaction_count", 0, 0, false),
		},
		"Open_files": &sql.MySQLStatusCounter{
			Name:  "Open_files",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Open_files", 0, 0, false),
		},
		"Open_streams": &sql.MySQLStatusCounter{
			Name:  "Open_streams",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Open_streams", 0, 0, false),
		},
		"Open_table_definitions": &sql.MySQLStatusCounter{
			Name:  "Open_table_definitions",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Open_table_definitions", 0, 0, false),
		},
		"Open_tables": &sql.MySQLStatusCounter{
			Name:  "Open_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Open_tables", 0, 0, false),
		},
		"Opened_files": &sql.MySQLStatusCounter{
			Name:  "Opened_files",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Opened_files", 0, 0, false),
		},
		"Opened_table_definitions": &sql.MySQLStatusCounter{
			Name:  "Opened_table_definitions",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Opened_table_definitions", 0, 0, false),
		},
		"Opened_tables": &sql.MySQLStatusCounter{
			Name:  "Opened_tables",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Opened_tables", 0, 0, false),
		},
		"Performance_schema_accounts_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_accounts_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_accounts_lost", 0, 0, false),
		},
		"Performance_schema_cond_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_cond_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_cond_classes_lost", 0, 0, false),
		},
		"Performance_schema_cond_instances_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_cond_instances_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_cond_instances_lost", 0, 0, false),
		},
		"Performance_schema_digest_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_digest_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_digest_lost", 0, 0, false),
		},
		"Performance_schema_file_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_file_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_file_classes_lost", 0, 0, false),
		},
		"Performance_schema_file_handles_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_file_handles_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_file_handles_lost", 0, 0, false),
		},
		"Performance_schema_file_instances_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_file_instances_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_file_instances_lost", 0, 0, false),
		},
		"Performance_schema_hosts_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_hosts_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_hosts_lost", 0, 0, false),
		},
		"Performance_schema_index_stat_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_index_stat_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_index_stat_lost", 0, 0, false),
		},
		"Performance_schema_locker_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_locker_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_locker_lost", 0, 0, false),
		},
		"Performance_schema_memory_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_memory_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_memory_classes_lost", 0, 0, false),
		},
		"Performance_schema_metadata_lock_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_metadata_lock_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_metadata_lock_lost", 0, 0, false),
		},
		"Performance_schema_mutex_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_mutex_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_mutex_classes_lost", 0, 0, false),
		},
		"Performance_schema_mutex_instances_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_mutex_instances_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_mutex_instances_lost", 0, 0, false),
		},
		"Performance_schema_nested_statement_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_nested_statement_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_nested_statement_lost", 0, 0, false),
		},
		"Performance_schema_prepared_statements_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_prepared_statements_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_prepared_statements_lost", 0, 0, false),
		},
		"Performance_schema_program_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_program_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_program_lost", 0, 0, false),
		},
		"Performance_schema_rwlock_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_rwlock_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_rwlock_classes_lost", 0, 0, false),
		},
		"Performance_schema_rwlock_instances_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_rwlock_instances_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_rwlock_instances_lost", 0, 0, false),
		},
		"Performance_schema_session_connect_attrs_longest_seen": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_session_connect_attrs_longest_seen",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_session_connect_attrs_longest_seen", 0, 0, false),
		},
		"Performance_schema_session_connect_attrs_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_session_connect_attrs_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_session_connect_attrs_lost", 0, 0, false),
		},
		"Performance_schema_socket_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_socket_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_socket_classes_lost", 0, 0, false),
		},
		"Performance_schema_socket_instances_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_socket_instances_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_socket_instances_lost", 0, 0, false),
		},
		"Performance_schema_stage_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_stage_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_stage_classes_lost", 0, 0, false),
		},
		"Performance_schema_statement_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_statement_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_statement_classes_lost", 0, 0, false),
		},
		"Performance_schema_table_handles_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_table_handles_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_table_handles_lost", 0, 0, false),
		},
		"Performance_schema_table_instances_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_table_instances_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_table_instances_lost", 0, 0, false),
		},
		"Performance_schema_table_lock_stat_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_table_lock_stat_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_table_lock_stat_lost", 0, 0, false),
		},
		"Performance_schema_thread_classes_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_thread_classes_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_thread_classes_lost", 0, 0, false),
		},
		"Performance_schema_thread_instances_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_thread_instances_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_thread_instances_lost", 0, 0, false),
		},
		"Performance_schema_users_lost": &sql.MySQLStatusCounter{
			Name:  "Performance_schema_users_lost",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Performance_schema_users_lost", 0, 0, false),
		},
		"Prepared_stmt_count": &sql.MySQLStatusCounter{
			Name:  "Prepared_stmt_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Prepared_stmt_count", 0, 0, false),
		},
		"Queries": &sql.MySQLStatusCounter{
			Name:  "Queries",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Queries", 0, 0, false),
		},
		"Questions": &sql.MySQLStatusCounter{
			Name:  "Questions",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Questions", 0, 0, false),
		},
		"Replica_open_temp_tables": &sql.MySQLStatusCounter{
			Name:  "Replica_open_temp_tables",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Replica_open_temp_tables", 0, 0, false),
		},
		"Replica_rows_last_search_algorithm_used": &sql.MySQLStatusVariable{
			Name:    "Replica_rows_last_search_algorithm_used",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Replica_rows_last_search_algorithm_used"),
			Default: "",
		},
		"Resource_group_supported": &sql.MySQLStatusVariable{
			Name:    "Resource_group_supported",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Resource_group_supported"),
			Default: int8(0),
		},
		"Rewriter_number_loaded_rules": &sql.MySQLStatusCounter{
			Name:  "Rewriter_number_loaded_rules",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rewriter_number_loaded_rules", 0, 0, false),
		},
		"Rewriter_number_reloads": &sql.MySQLStatusCounter{
			Name:  "Rewriter_number_reloads",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rewriter_number_reloads", 0, 0, false),
		},
		"Rewriter_number_rewritten_queries": &sql.MySQLStatusCounter{
			Name:  "Rewriter_number_rewritten_queries",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rewriter_number_rewritten_queries", 0, 0, false),
		},
		"Rewriter_reload_error": &sql.MySQLStatusVariable{
			Name:    "Rewriter_reload_error",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Rewriter_reload_error"),
			Default: int8(0),
		},
		"Rpl_semi_sync_master_clients": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_clients",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_clients", 0, 0, false),
		},
		"Rpl_semi_sync_master_net_avg_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_net_avg_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_net_avg_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_master_net_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_net_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_net_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_master_net_waits": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_net_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_net_waits", 0, 0, false),
		},
		"Rpl_semi_sync_master_no_times": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_no_times",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_no_times", 0, 0, false),
		},
		"Rpl_semi_sync_master_no_tx": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_no_tx",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_no_tx", 0, 0, false),
		},
		"Rpl_semi_sync_master_status": &sql.MySQLStatusVariable{
			Name:    "Rpl_semi_sync_master_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Rpl_semi_sync_master_status"),
			Default: int8(0),
		},
		"Rpl_semi_sync_master_timefunc_failures": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_timefunc_failures",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_timefunc_failures", 0, 0, false),
		},
		"Rpl_semi_sync_master_tx_avg_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_tx_avg_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_tx_avg_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_master_tx_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_tx_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_tx_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_master_tx_waits": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_tx_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_tx_waits", 0, 0, false),
		},
		"Rpl_semi_sync_master_wait_pos_backtraverse": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_wait_pos_backtraverse",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_wait_pos_backtraverse", 0, 0, false),
		},
		"Rpl_semi_sync_master_wait_sessions": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_wait_sessions",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_wait_sessions", 0, 0, false),
		},
		"Rpl_semi_sync_master_yes_tx": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_master_yes_tx",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_master_yes_tx", 0, 0, false),
		},
		"Rpl_semi_sync_replica_status": &sql.MySQLStatusVariable{
			Name:    "Rpl_semi_sync_replica_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Rpl_semi_sync_replica_status"),
			Default: int8(0),
		},
		"Rpl_semi_sync_slave_status": &sql.MySQLStatusVariable{
			Name:    "Rpl_semi_sync_slave_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Rpl_semi_sync_slave_status"),
			Default: int8(0),
		},
		"Rpl_semi_sync_source_clients": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_clients",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_clients", 0, 0, false),
		},
		"Rpl_semi_sync_source_net_avg_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_net_avg_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_net_avg_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_source_net_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_net_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_net_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_source_net_waits": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_net_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_net_waits", 0, 0, false),
		},
		"Rpl_semi_sync_source_no_times": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_no_times",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_no_times", 0, 0, false),
		},
		"Rpl_semi_sync_source_no_tx": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_no_tx",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_no_tx", 0, 0, false),
		},
		"Rpl_semi_sync_source_status": &sql.MySQLStatusVariable{
			Name:    "Rpl_semi_sync_source_status",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Rpl_semi_sync_source_status"),
			Default: int8(0),
		},
		"Rpl_semi_sync_source_timefunc_failures": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_timefunc_failures",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_timefunc_failures", 0, 0, false),
		},
		"Rpl_semi_sync_source_tx_avg_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_tx_avg_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_tx_avg_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_source_tx_wait_time": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_tx_wait_time",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_tx_wait_time", 0, 0, false),
		},
		"Rpl_semi_sync_source_tx_waits": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_tx_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_tx_waits", 0, 0, false),
		},
		"Rpl_semi_sync_source_wait_pos_backtraverse": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_wait_pos_backtraverse",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_wait_pos_backtraverse", 0, 0, false),
		},
		"Rpl_semi_sync_source_wait_sessions": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_wait_sessions",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_wait_sessions", 0, 0, false),
		},
		"Rpl_semi_sync_source_yes_tx": &sql.MySQLStatusCounter{
			Name:  "Rpl_semi_sync_source_yes_tx",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Rpl_semi_sync_source_yes_tx", 0, 0, false),
		},
		"Rsa_public_key": &sql.MySQLStatusVariable{
			Name:    "Rsa_public_key",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Rsa_public_key"),
			Default: "",
		},
		"Secondary_engine_execution_count": &sql.MySQLStatusCounter{
			Name:  "Secondary_engine_execution_count",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Secondary_engine_execution_count", 0, 0, false),
		},
		"Select_full_join": &sql.MySQLStatusCounter{
			Name:  "Select_full_join",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Select_full_join", 0, 0, false),
		},
		"Select_full_range_join": &sql.MySQLStatusCounter{
			Name:  "Select_full_range_join",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Select_full_range_join", 0, 0, false),
		},
		"Select_range": &sql.MySQLStatusCounter{
			Name:  "Select_range",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Select_range", 0, 0, false),
		},
		"Select_range_check": &sql.MySQLStatusCounter{
			Name:  "Select_range_check",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Select_range_check", 0, 0, false),
		},
		"Select_scan": &sql.MySQLStatusCounter{
			Name:  "Select_scan",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Select_scan", 0, 0, false),
		},
		"Slave_open_temp_tables": &sql.MySQLStatusCounter{
			Name:  "Slave_open_temp_tables",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Slave_open_temp_tables", 0, 0, false),
		},
		"Slave_rows_last_search_algorithm_used": &sql.MySQLStatusVariable{
			Name:    "Slave_rows_last_search_algorithm_used",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Slave_rows_last_search_algorithm_used"),
			Default: "",
		},
		"Slow_launch_threads": &sql.MySQLStatusCounter{
			Name:  "Slow_launch_threads",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Slow_launch_threads", 0, 0, false),
		},
		"Slow_queries": &sql.MySQLStatusCounter{
			Name:  "Slow_queries",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Slow_queries", 0, 0, false),
		},
		"Sort_merge_passes": &sql.MySQLStatusCounter{
			Name:  "Sort_merge_passes",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Sort_merge_passes", 0, 0, false),
		},
		"Sort_range": &sql.MySQLStatusCounter{
			Name:  "Sort_range",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Sort_range", 0, 0, false),
		},
		"Sort_rows": &sql.MySQLStatusCounter{
			Name:  "Sort_rows",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Sort_rows", 0, 0, false),
		},
		"Sort_scan": &sql.MySQLStatusCounter{
			Name:  "Sort_scan",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Sort_scan", 0, 0, false),
		},
		"Ssl_accept_renegotiates": &sql.MySQLStatusCounter{
			Name:  "Ssl_accept_renegotiates",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_accept_renegotiates", 0, 0, false),
		},
		"Ssl_accepts": &sql.MySQLStatusCounter{
			Name:  "Ssl_accepts",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_accepts", 0, 0, false),
		},
		"Ssl_callback_cache_hits": &sql.MySQLStatusCounter{
			Name:  "Ssl_callback_cache_hits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_callback_cache_hits", 0, 0, false),
		},
		"Ssl_cipher": &sql.MySQLStatusVariable{
			Name:    "Ssl_cipher",
			Scope:   sql.StatusVariableScope_Both,
			Type:    types.NewSystemStringType("Ssl_cipher"),
			Default: "",
		},
		"Ssl_cipher_list": &sql.MySQLStatusVariable{
			Name:    "Ssl_cipher_list",
			Scope:   sql.StatusVariableScope_Both,
			Type:    types.NewSystemStringType("Ssl_cipher_list"),
			Default: "",
		},
		"Ssl_client_connects": &sql.MySQLStatusCounter{
			Name:  "Ssl_client_connects",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_client_connects", 0, 0, false),
		},
		"Ssl_connect_renegotiates": &sql.MySQLStatusCounter{
			Name:  "Ssl_connect_renegotiates",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_connect_renegotiates", 0, 0, false),
		},
		"Ssl_ctx_verify_depth": &sql.MySQLStatusCounter{
			Name:  "Ssl_ctx_verify_depth",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_ctx_verify_depth", 0, 0, false),
		},
		"Ssl_ctx_verify_mode": &sql.MySQLStatusCounter{
			Name:  "Ssl_ctx_verify_mode",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_ctx_verify_mode", 0, 0, false),
		},
		"Ssl_default_timeout": &sql.MySQLStatusCounter{
			Name:  "Ssl_default_timeout",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ssl_default_timeout", 0, 0, false),
		},
		"Ssl_finished_accepts": &sql.MySQLStatusCounter{
			Name:  "Ssl_finished_accepts",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_finished_accepts", 0, 0, false),
		},
		"Ssl_finished_connects": &sql.MySQLStatusCounter{
			Name:  "Ssl_finished_connects",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_finished_connects", 0, 0, false),
		},
		"Ssl_server_not_after": &sql.MySQLStatusCounter{
			Name:  "Ssl_server_not_after",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ssl_server_not_after", 0, 0, false),
		},
		"Ssl_server_not_before": &sql.MySQLStatusCounter{
			Name:  "Ssl_server_not_before",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ssl_server_not_before", 0, 0, false),
		},
		"Ssl_session_cache_hits": &sql.MySQLStatusCounter{
			Name:  "Ssl_session_cache_hits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_session_cache_hits", 0, 0, false),
		},
		"Ssl_session_cache_misses": &sql.MySQLStatusCounter{
			Name:  "Ssl_session_cache_misses",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_session_cache_misses", 0, 0, false),
		},
		"Ssl_session_cache_mode": &sql.MySQLStatusVariable{
			Name:    "Ssl_session_cache_mode",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Ssl_session_cache_mode"),
			Default: "",
		},
		"Ssl_session_cache_overflows": &sql.MySQLStatusCounter{
			Name:  "Ssl_session_cache_overflows",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_session_cache_overflows", 0, 0, false),
		},
		"Ssl_session_cache_size": &sql.MySQLStatusCounter{
			Name:  "Ssl_session_cache_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_session_cache_size", 0, 0, false),
		},
		"Ssl_session_cache_timeout": &sql.MySQLStatusCounter{
			Name:  "Ssl_session_cache_timeout",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_session_cache_timeout", 0, 0, false),
		},
		"Ssl_session_cache_timeouts": &sql.MySQLStatusCounter{
			Name:  "Ssl_session_cache_timeouts",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_session_cache_timeouts", 0, 0, false),
		},
		"Ssl_sessions_reused": &sql.MySQLStatusCounter{
			Name:  "Ssl_sessions_reused",
			Scope: sql.StatusVariableScope_Session,
			Type:  types.NewSystemIntType("Ssl_sessions_reused", 0, 0, false),
		},
		"Ssl_used_session_cache_entries": &sql.MySQLStatusCounter{
			Name:  "Ssl_used_session_cache_entries",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Ssl_used_session_cache_entries", 0, 0, false),
		},
		"Ssl_verify_depth": &sql.MySQLStatusCounter{
			Name:  "Ssl_verify_depth",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ssl_verify_depth", 0, 0, false),
		},
		"Ssl_verify_mode": &sql.MySQLStatusCounter{
			Name:  "Ssl_verify_mode",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Ssl_verify_mode", 0, 0, false),
		},
		"Ssl_version": &sql.MySQLStatusVariable{
			Name:    "Ssl_version",
			Scope:   sql.StatusVariableScope_Both,
			Type:    types.NewSystemStringType("Ssl_version"),
			Default: "",
		},
		"Table_locks_immediate": &sql.MySQLStatusCounter{
			Name:  "Table_locks_immediate",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Table_locks_immediate", 0, 0, false),
		},
		"Table_locks_waited": &sql.MySQLStatusCounter{
			Name:  "Table_locks_waited",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Table_locks_waited", 0, 0, false),
		},
		"Table_open_cache_hits": &sql.MySQLStatusCounter{
			Name:  "Table_open_cache_hits",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Table_open_cache_hits", 0, 0, false),
		},
		"Table_open_cache_misses": &sql.MySQLStatusCounter{
			Name:  "Table_open_cache_misses",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Table_open_cache_misses", 0, 0, false),
		},
		"Table_open_cache_overflows": &sql.MySQLStatusCounter{
			Name:  "Table_open_cache_overflows",
			Scope: sql.StatusVariableScope_Both,
			Type:  types.NewSystemIntType("Table_open_cache_overflows", 0, 0, false),
		},
		"Tc_log_max_pages_used": &sql.MySQLStatusCounter{
			Name:  "Tc_log_max_pages_used",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Tc_log_max_pages_used", 0, 0, false),
		},
		"Tc_log_page_size": &sql.MySQLStatusCounter{
			Name:  "Tc_log_page_size",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Tc_log_page_size", 0, 0, false),
		},
		"Tc_log_page_waits": &sql.MySQLStatusCounter{
			Name:  "Tc_log_page_waits",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Tc_log_page_waits", 0, 0, false),
		},
		"Telemetry_traces_supported": &sql.MySQLStatusVariable{
			Name:    "Telemetry_traces_supported",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemBoolType("Telemetry_traces_supported"),
			Default: int8(0),
		},
		"Threads_cached": &sql.MySQLStatusCounter{
			Name:  "Threads_cached",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Threads_cached", 0, 0, false),
		},
		"Threads_connected": &sql.MySQLStatusCounter{
			Name:  "Threads_connected",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Threads_connected", 0, 0, false),
		},
		"Threads_created": &sql.MySQLStatusCounter{
			Name:  "Threads_created",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Threads_created", 0, 0, false),
		},
		"Threads_running": &sql.MySQLStatusCounter{
			Name:  "Threads_running",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Threads_running", 0, 0, false),
		},
		"Tls_library_version": &sql.MySQLStatusVariable{
			Name:    "Tls_library_version",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("Tls_library_version"),
			Default: "",
		},
		"Uptime": &sql.MySQLStatusCounter{
			Name:  "Uptime",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Uptime", 0, 0, false),
		},
		"Uptime_since_flush_status": &sql.MySQLStatusCounter{
			Name:  "Uptime_since_flush_status",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("Uptime_since_flush_status", 0, 0, false),
		},
		"validate_password_dictionary_file_last_parsed": &sql.MySQLStatusVariable{
			Name:    "validate_password_dictionary_file_last_parsed",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("validate_password_dictionary_file_last_parsed"),
			Default: "",
		},
		"validate_password_dictionary_file_words_count": &sql.MySQLStatusCounter{
			Name:  "validate_password_dictionary_file_words_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("validate_password_dictionary_file_words_count", 0, 0, false),
		},
		"validate_password.dictionary_file_last_parsed": &sql.MySQLStatusVariable{
			Name:    "validate_password.dictionary_file_last_parsed",
			Scope:   sql.StatusVariableScope_Global,
			Type:    types.NewSystemStringType("validate_password.dictionary_file_last_parsed"),
			Default: "",
		},
		"validate_password.dictionary_file_words_count": &sql.MySQLStatusCounter{
			Name:  "validate_password.dictionary_file_words_count",
			Scope: sql.StatusVariableScope_Global,
			Type:  types.NewSystemIntType("validate_password.dictionary_file_words_count", 0, 0, false),
		},
	}
}
