package support

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestMemoGen(t *testing.T) {
	test := struct {
		expected string
	}{
		expected: `
        import (
          "fmt"
          "strings"
          "github.com/dolthub/go-mysql-server/sql"
          "github.com/dolthub/go-mysql-server/sql/plan"
        )
        
        type hashJoin struct {
          *joinBase
          innerAttrs []sql.Expression
          outerAttrs []sql.Expression
        }
        
        var _ relExpr = (*hashJoin)(nil)
        var _ joinRel = (*hashJoin)(nil)
        
        func (r *hashJoin) String() string {
          return formatRelExpr(r)
        }
        
        func (r *hashJoin) joinPrivate() *joinBase {
          return r.joinBase
        }
        
        type tableScan struct {
          *relBase
          table *plan.ResolvedTable
        }
        
        var _ relExpr = (*tableScan)(nil)
        var _ sourceRel = (*tableScan)(nil)
        
        func (r *tableScan) String() string {
          return formatRelExpr(r)
        }
        
        func (r *tableScan) name() string {
          return strings.ToLower(r.table.Name())
        }
        
        func (r *tableScan) tableId() TableId {
          return tableIdForSource(r.g.id)
        }
        
        func (r *tableScan) children() []*exprGroup {
          return nil
        }
        
        func (r *tableScan) outputCols() sql.Schema {
          return r.table.Schema()
        }
        
        func formatRelExpr(r relExpr) string {
          switch r := r.(type) {
          case *hashJoin:
            return fmt.Sprintf("hashJoin %d %d", r.left.id, r.right.id)
          case *tableScan:
            return fmt.Sprintf("tableScan: %s", r.name())
          default:
            panic(fmt.Sprintf("unknown relExpr type: %T", r))
          }
        }
        
        func buildRelExpr(b *ExecBuilder, r relExpr, input sql.Schema, children ...sql.Node) (sql.Node, error) {
          var result sql.Node
          var err error
        
          switch r := r.(type) {
          case *hashJoin:
          result, err = b.buildHashJoin(r, input, children...)
          case *tableScan:
          result, err = b.buildTableScan(r, input, children...)
          default:
            panic(fmt.Sprintf("unknown relExpr type: %T", r))
          }
        
          if err != nil {
            return nil, err
          }
        
          result, err = r.group().finalize(result, input)
          if err != nil {
            return nil, err
          }
          return result, nil
        }
		`,
	}

	defs := []MemoDef{
		{
			Name:   "hashJoin",
			IsJoin: true,
			Attrs: [][2]string{
				{"innerAttrs", "[]sql.Expression"},
				{"outerAttrs", "[]sql.Expression"},
			},
		},
		{
			Name:       "tableScan",
			SourceType: "*plan.ResolvedTable",
		},
	}
	gen := MemoGen{}
	var buf bytes.Buffer
	gen.Generate(defs, &buf)

	if testing.Verbose() {
		fmt.Printf("\n=>\n\n%s\n", buf.String())
	}

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(test.expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", test.expected, buf.String())
	}
}
