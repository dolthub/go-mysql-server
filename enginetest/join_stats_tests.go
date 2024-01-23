package enginetest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
)

func TestJoinStats(t *testing.T, harness Harness) {
	harness.Setup(setup.MydbData)

	for _, tt := range JoinStatTests {
		t.Run(tt.name, func(t *testing.T) {
			harness.Setup([]setup.SetupScript{setup.MydbData[0]})
			e := mustNewEngine(t, harness)
			defer e.Close()

			tfp, ok := e.EngineAnalyzer().Catalog.DbProvider.(sql.TableFunctionProvider)
			if !ok {
				return
			}
			newPro, err := tfp.WithTableFunctions(memory.ExponentialDistTable{}, memory.NormalDistTable{})
			require.NoError(t, err)
			e.EngineAnalyzer().Catalog.DbProvider = newPro.(sql.DatabaseProvider)

			ctx := harness.NewContext()
			for _, q := range tt.setup {
				_, iter, err := e.Query(ctx, q)
				require.NoError(t, err)
				_, err = sql.RowIterToRows(ctx, iter)
				require.NoError(t, err)
			}

			for _, tt := range tt.tests {
				if tt.order != nil {
					evalJoinOrder(t, harness, e, tt.q, tt.order, tt.skipOld)
				}
				if tt.exp != nil {
					evalJoinCorrectness(t, harness, e, tt.q, tt.q, tt.exp, false)
				}
			}
		})
	}
}

var JoinStatTests = []struct {
	name  string
	setup []string
	tests []JoinPlanTest
}{
	{
		name: "test table orders with normal distributions",
		setup: []string{
			"create table u0 (a int primary key, b int, c int, key (b,c))",
			"insert into u0 select * from normal_dist(2, 500, 0, 5)",
			"create table u0_2 (a int primary key, b int, c int, key (b,c))",
			"insert into u0_2 select * from normal_dist(2, 3000, 0, 5)",
			"create table `u-15` (a int primary key, b int, c int, key (b,c))",
			"insert into `u-15` select * from normal_dist(2, 5000, -15, 5)",
			"create table `u+15` (a int primary key, b int, c int, key (b,c))",
			"insert into `u+15` select * from normal_dist(2, 6000, 15, 5)",
			"analyze table u0",
			"analyze table u0_2",
			"analyze table `u-15`",
			"analyze table `u+15`",
		},
		tests: []JoinPlanTest{
			{
				// a is smaller
				q:     "select /*+ LEFT_DEEP */ count(*) from `u-15` a join `u+15` b on a.b = b.b",
				order: [][]string{{"a", "b"}},
			},
			{
				// b with filter is smaller
				q:     "select /*+ LEFT_DEEP */ count(*) from `u-15` a join `u+15` b on a.b = b.b where b.b < 15",
				order: [][]string{{"b", "a"}},
			},
		},
	},
	{
		name: "test table orders with exponential distributions",
		setup: []string{
			"create table mid (a int primary key, b int, c int, key (b,c))",
			"insert into mid select * from normal_dist(2, 1000, 9, 1)",
			"create table low (a int primary key, b int, c int, key (b,c))",
			"insert into low select * from exponential_dist(2, 2000, .1)",
			"create table high (a int primary key, b int, c int, key (b,c))",
			"insert into high select col0, 10-col1, col2 from exponential_dist(2, 4000, .1)",
			"analyze table low, mid, high",
		},
		tests: []JoinPlanTest{
			{
				// low is flattish exponential upwards from 0->
				// high is flattish exponential downwards from <-10
				// mid is sharp normal near high
				// order (mid x low) x high is the easiest and expected
				// for certain seeds, (low, high) will be smaller than mid and chosen
				q:     "select /*+ LEFT_DEEP LOOKUP_JOIN(low, high) LOOKUP_JOIN(mid, high) */ count(*) from low join mid  on low.b = mid.b join high on low.b = high.b",
				order: [][]string{{"mid", "low", "high"}, {"low", "high", "mid"}},
			},
		},
	},
	{
		// there is a trade-off for these where we either pick the first table
		// first if card(b) < card(axc), or we choose (axc) if its intermediate
		// result cardinality is smaller than filtered (b).
		name: "test table orders with filters and normal distributions",
		setup: []string{
			"create table u0 (a int primary key, b int, c int, key (b,c))",
			"insert into u0 select * from normal_dist(2, 2000, 0, 5)",
			"create table u0_2 (a int primary key, b int, c int, key (b,c))",
			"insert into u0_2 select * from normal_dist(2, 2000, 0, 5)",
			"create table `u-15` (a int primary key, b int, c int, key (b,c))",
			"insert into `u-15` select * from normal_dist(2, 2000, -10, 5)",
			"create table `u+15` (a int primary key, b int, c int, key (b,c))",
			"insert into `u+15` select * from normal_dist(2, 2000, 10, 5)",
			"analyze table u0",
			"analyze table u0_2",
			"analyze table `u-15`",
			"analyze table `u+15`",
		},
		tests: []JoinPlanTest{
			{
				// axc is smallest join, a is smallest table
				// (axb) is less than (axc) maybe 1/50 times
				q:     "select /*+ LEFT_DEEP LOOKUP_JOIN(a,b) LOOKUP_JOIN(b,c) */  count(*) from u0 b join `u-15` a on a.b = b.b join `u+15` c on a.b = c.b where a.b > 3",
				order: [][]string{{"a", "c", "b"}, {"a", "b", "c"}},
			},
			{
				// b is smallest table, bxc is smallest b-connected join
				// due to b < 0 filter and positive c skew
				q:     "select /*+ LEFT_DEEP */  count(*) from u0 b join `u-15` a on a.b = b.b join `u+15` c on a.b = c.b where b.b < -2",
				order: [][]string{{"b", "c", "a"}},
			},
			{
				q:     "select /*+ LEFT_DEEP */ count(*) from u0 b join `u-15` a on a.b = b.b join `u+15` c on a.b = c.b where b.b < -2",
				order: [][]string{{"b", "c", "a"}},
			},
			{
				// b is smallest table, bxa is smallest b-connected join
				// due to b > 0 filter and negative c skew
				// for certain seeds (cxa) is much smaller that b
				q:     "select /*+ LEFT_DEEP LOOKUP_JOIN(a,b) LOOKUP_JOIN(b,c) */ count(*) from `u-15` a join u0 b on a.b = b.b join `u+15` c on a.b = c.b where b.b > 2",
				order: [][]string{{"b", "a", "c"}, {"c", "a", "b"}},
			},
			{
				q:     "select /*+ LEFT_DEEP LOOKUP_JOIN(a,b) LOOKUP_JOIN(b,c) */ count(*) from u0 b join `u-15` a on a.b = b.b join `u+15` c on a.b = c.b where b.b > 2",
				order: [][]string{{"b", "a", "c"}, {"c", "a", "b"}},
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
