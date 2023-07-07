package planbuilder

import (
	"fmt"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/plan"
	ast "github.com/dolthub/vitess/go/vt/sqlparser"
)

func (b *PlanBuilder) buildChangeReplicationSource(inScope *scope, n *ast.ChangeReplicationSource) (outScope *scope) {
	outScope = inScope.push()
	convertedOptions := make([]binlogreplication.ReplicationOption, 0, len(n.Options))
	for _, option := range n.Options {
		convertedOption := b.buildReplicationOption(inScope, option)
		convertedOptions = append(convertedOptions, *convertedOption)
	}
	outScope.node = plan.NewChangeReplicationSource(convertedOptions)
	return outScope
}

func (b *PlanBuilder) buildReplicationOption(inScope *scope, option *ast.ReplicationOption) *binlogreplication.ReplicationOption {
	if option.Value == nil {
		err := fmt.Errorf("nil replication option specified for option %q", option.Name)
		b.handleErr(err)
	}
	switch vv := option.Value.(type) {
	case string:
		return binlogreplication.NewReplicationOption(option.Name, binlogreplication.StringReplicationOptionValue{Value: vv})
	case int:
		return binlogreplication.NewReplicationOption(option.Name, binlogreplication.IntegerReplicationOptionValue{Value: vv})
	case ast.TableNames:
		urts := make([]sql.UnresolvedTable, len(vv))
		for i, tableName := range vv {
			//db := b.currentDatabase.Name()
			//if tableName.Qualifier.String() != "" {
			//	db = tableName.Qualifier.String()
			//}
			//urts[i] = b.resolveTable(tableName.Name.String(), db, nil)
			urts[i] = plan.NewUnresolvedTable(tableName.Name.String(), tableName.Qualifier.String())
		}
		return binlogreplication.NewReplicationOption(option.Name, binlogreplication.TableNamesReplicationOptionValue{Value: urts})
	default:
		err := fmt.Errorf("unsupported option value type '%T' specified for option %q", option.Value, option.Name)
		b.handleErr(err)
	}
	return nil
}

func (b *PlanBuilder) buildChangeReplicationFilter(inScope *scope, n *ast.ChangeReplicationFilter) (outScope *scope) {
	outScope = inScope.push()
	convertedOptions := make([]binlogreplication.ReplicationOption, 0, len(n.Options))
	for _, option := range n.Options {
		convertedOption := b.buildReplicationOption(inScope, option)
		convertedOptions = append(convertedOptions, *convertedOption)
	}
	outScope.node = plan.NewChangeReplicationFilter(convertedOptions)
	return outScope
}
