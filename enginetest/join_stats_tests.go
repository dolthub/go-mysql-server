package enginetest

import (
	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestJoinStats(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)

	e := mustNewEngine(t, harness)
	tfp, ok := e.EngineAnalyzer().Catalog.DbProvider.(sql.TableFunctionProvider)
	if !ok {
		return
	}
	newPro, err := tfp.WithTableFunctions(memory.ExponentialDistTable{}, memory.NormalDistTable{})
	require.NoError(t, err)
	e.EngineAnalyzer().Catalog.DbProvider = newPro.(sql.DatabaseProvider)

	for _, test := range EngineOnlyJoinStatTests {
		TestScriptWithEngine(t, e, harness, test)
	}
}

var EngineOnlyJoinStatTests = []queries.ScriptTest{
	{
		Name: "two normal distributions",
		SetUpScript: []string{
			"create table norm1 (a int primary key, b int, c int, key (b,c))",
			"insert into norm1 select * from normal_dist(2, 1000, 0, 10)",
			"create table norm2 (d int primary key, e int, f int, key (e,f))",
			"insert into norm2 select * from normal_dist(2, 1000, 0, 10)",
			"analyze table norm1",
			"analyze table norm2",
		},
		Assertions: []queries.ScriptTestAssertion{
			{
				Query:    "select count(*) from norm1 join norm2 on b = e",
				Expected: []sql.Row{},
			},
			{
				Query:    "select count(*) from norm1 join norm2 on b = e where a  < 0",
				Expected: []sql.Row{},
			},
			{
				Query:    "explain select count(*) from norm1 join norm2 on b = e",
				Expected: []sql.Row{},
			},
			{
				Query:    "explain select count(*) from norm1 join norm2 on b = e where b  < 0",
				Expected: []sql.Row{},
			},
		},
	},
}

func NewTestProvider(dbProvider *sql.MutableDatabaseProvider, tf ...sql.TableFunction) *TestProvider {
	tfs := make(map[string]sql.TableFunction)
	for _, tf := range tf {
		tfs[strings.ToLower(tf.Name())] = tf
	}
	return &TestProvider{
		*dbProvider,
		tfs,
	}
}

var _ sql.FunctionProvider = (*TestProvider)(nil)

type TestProvider struct {
	sql.MutableDatabaseProvider
	tableFunctions map[string]sql.TableFunction
}

func (t TestProvider) Function(_ *sql.Context, name string) (sql.Function, error) {
	return nil, sql.ErrFunctionNotFound.New(name)
}

func (t TestProvider) TableFunction(_ *sql.Context, name string) (sql.TableFunction, error) {
	if tf, ok := t.tableFunctions[strings.ToLower(name)]; ok {
		return tf, nil
	}

	return nil, sql.ErrTableFunctionNotFound.New(name)
}

func (t TestProvider) WithTableFunctions(fns ...sql.TableFunction) (sql.TableFunctionProvider, error) {
	return t, nil
}
