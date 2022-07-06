package enginetest

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/enginetest/queries"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql/parse"
)

// This test will write a new set of query plan expected results to a file that you can copy and paste over the existing
// query plan results. Handy when you've made a large change to the analyzer or node formatting, and you want to examine
// how query plans have changed without a lot of manual copying and pasting.
func TestWriteQueryPlans(t *testing.T) {
	t.Skip()

	harness := NewDefaultMemoryHarness()
	harness.Setup(setup.SimpleSetup...)
	engine := mustNewEngine(t, harness)

	tmp, err := ioutil.TempDir("", "*")
	if err != nil {
		return
	}

	outputPath := filepath.Join(tmp, "queryPlans.txt")
	f, err := os.Create(outputPath)
	require.NoError(t, err)

	w := bufio.NewWriter(f)
	_, _ = w.WriteString("var PlanTests = []QueryPlanTest{\n")
	for _, tt := range queries.PlanTests {
		_, _ = w.WriteString("\t{\n")
		ctx := NewContextWithEngine(harness, engine)
		parsed, err := parse.Parse(ctx, tt.Query)
		require.NoError(t, err)

		node, err := engine.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		planString := ExtractQueryNode(node).String()

		if strings.Contains(tt.Query, "`") {
			_, _ = w.WriteString(fmt.Sprintf(`Query: "%s",`, tt.Query))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("Query: `%s`,", tt.Query))
		}
		_, _ = w.WriteString("\n")

		_, _ = w.WriteString(`ExpectedPlan: `)
		for i, line := range strings.Split(planString, "\n") {
			if i > 0 {
				_, _ = w.WriteString(" + \n")
			}
			if len(line) > 0 {
				_, _ = w.WriteString(fmt.Sprintf(`"%s\n"`, strings.ReplaceAll(line, `"`, `\"`)))
			} else {
				// final line with comma
				_, _ = w.WriteString("\"\",\n")
			}
		}
		_, _ = w.WriteString("\t},\n")
	}
	_, _ = w.WriteString("}")

	_ = w.Flush()

	t.Logf("Query plans in %s", outputPath)
}

func TestWriteIndexQueryPlans(t *testing.T) {
	t.Skip()

	harness := NewDefaultMemoryHarness()
	engine := mustNewEngine(t, harness)

	CreateIndexes(t, harness, engine)

	tmp, err := ioutil.TempDir("", "*")
	if err != nil {
		return
	}

	outputPath := filepath.Join(tmp, "indexQueryPlans.txt")
	f, err := os.Create(outputPath)
	require.NoError(t, err)

	w := bufio.NewWriter(f)
	_, _ = w.WriteString("var IndexPlanTests = []QueryPlanTest{\n")
	for _, tt := range queries.IndexPlanTests {
		_, _ = w.WriteString("\t{\n")
		ctx := NewContextWithEngine(harness, engine)
		parsed, err := parse.Parse(ctx, tt.Query)
		require.NoError(t, err)

		node, err := engine.Analyzer.Analyze(ctx, parsed, nil)
		require.NoError(t, err)
		planString := ExtractQueryNode(node).String()

		if strings.Contains(tt.Query, "`") {
			_, _ = w.WriteString(fmt.Sprintf(`Query: "%s",`, tt.Query))
		} else {
			_, _ = w.WriteString(fmt.Sprintf("Query: `%s`,", tt.Query))
		}
		_, _ = w.WriteString("\n")

		_, _ = w.WriteString(`ExpectedPlan: `)
		for i, line := range strings.Split(planString, "\n") {
			if i > 0 {
				_, _ = w.WriteString(" + \n")
			}
			if len(line) > 0 {
				_, _ = w.WriteString(fmt.Sprintf(`"%s\n"`, strings.ReplaceAll(line, `"`, `\"`)))
			} else {
				// final line with comma
				_, _ = w.WriteString("\"\",\n")
			}
		}
		_, _ = w.WriteString("\t},\n")
	}
	_, _ = w.WriteString("}")

	_ = w.Flush()

	t.Logf("Query plans in %s", outputPath)
}

func TestWriteComplexIndexQueries(t *testing.T) {
	t.Skip()
	tmp, err := ioutil.TempDir("", "*")
	if err != nil {
		return
	}

	outputPath := filepath.Join(tmp, "complex_index_queries.txt")
	f, err := os.Create(outputPath)
	require.NoError(t, err)

	w := bufio.NewWriter(f)
	_, _ = w.WriteString("var ComplexIndexQueries = []QueryTest{\n")
	for _, tt := range queries.ComplexIndexQueries {
		w.WriteString("  {\n")
		w.WriteString(fmt.Sprintf("    Query: `%s`,\n", tt.Query))
		w.WriteString(fmt.Sprintf("    Expected: %#v,\n", tt.Expected))
		w.WriteString("  },\n")
	}
	w.WriteString("}\n")
	w.Flush()
	t.Logf("Query tests in:\n %s", outputPath)
}

func TestWriteCreateTableQueries(t *testing.T) {
	t.Skip()
	tmp, err := ioutil.TempDir("", "*")
	if err != nil {
		return
	}

	outputPath := filepath.Join(tmp, "create_table_queries.txt")
	f, err := os.Create(outputPath)
	require.NoError(t, err)

	harness := NewDefaultMemoryHarness()
	harness.Setup(setup.MydbData, setup.MytableData, setup.FooData)

	w := bufio.NewWriter(f)
	_, _ = w.WriteString("var CreateTableQueries = []WriteQueryTest{\n")
	for _, tt := range queries.CreateTableQueries {
		ctx := NewContext(harness)
		engine := mustNewEngine(t, harness)
		_, _ = MustQuery(ctx, engine, tt.WriteQuery)
		_, res := MustQuery(ctx, engine, tt.SelectQuery)

		w.WriteString("  {\n")
		w.WriteString(fmt.Sprintf("    WriteQuery:`%s`,\n", tt.WriteQuery))
		w.WriteString(fmt.Sprintf("    ExpectedWriteResult: %#v,\n", tt.ExpectedWriteResult))
		w.WriteString(fmt.Sprintf("    SelectQuery: \"%s\",\n", tt.SelectQuery))
		w.WriteString(fmt.Sprintf("    ExpectedSelect: %#v,\n", res))
		w.WriteString("  },\n")
	}
	w.WriteString("}\n")
	w.Flush()
	t.Logf("Query tests in:\n %s", outputPath)

}
