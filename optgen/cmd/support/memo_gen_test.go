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
        
        type HashJoin struct {
          *JoinBase
          InnerAttrs []sql.Expression
          OuterAttrs []sql.Expression
        }
        
        var _ RelExpr = (*hashJoin)(nil)
        var _ JoinRel = (*hashJoin)(nil)
        
        func (r *hashJoin) String() string {
          return FormatExpr(r)
        }
        
        func (r *hashJoin) JoinPrivate() *JoinBase {
          return r.JoinBase
        }
        
        type TableScan struct {
          *sourceBase
          Table *plan.TableNode
        }
        
        var _ RelExpr = (*tableScan)(nil)
        var _ SourceRel = (*tableScan)(nil)
        
        func (r *tableScan) String() string {
          return FormatExpr(r)
        }
        
        func (r *tableScan) Name() string {
          return strings.ToLower(r.Table.Name())
        }
        
        func (r *tableScan) TableId() sql.TableId {
          return TableIdForSource(r.g.Id)
        }
        
        func (r *tableScan) TableIdNode() plan.TableIdNode {
          return r.Table
        }
        
        func (r *tableScan) OutputCols() sql.Schema {
          return r.Table.Schema()
        }
        
        func (r *tableScan) Children() []*ExprGroup {
          return nil
        }
        
        func FormatExpr(r exprType) string {
          switch r := r.(type) {
          case *hashJoin:
            return fmt.Sprintf("hashjoin %d %d", r.Left.Id, r.Right.Id)
          case *tableScan:
            return fmt.Sprintf("tablescan: %s", r.Name())
          default:
            panic(fmt.Sprintf("unknown RelExpr type: %T", r))
          }
        }
        
        func buildRelExpr(b *ExecBuilder, r RelExpr, children ...sql.Node) (sql.Node, error) {
          var result sql.Node
          var err error
        
          switch r := r.(type) {
          case *hashJoin:
          result, err = b.buildHashJoin(r, children...)
          case *tableScan:
          result, err = b.buildTableScan(r, children...)
          default:
            panic(fmt.Sprintf("unknown RelExpr type: %T", r))
          }
        
          if err != nil {
            return nil, err
          }
        
          if withDescribeStats, ok := result.(sql.WithDescribeStats); ok {
            withDescribeStats.SetDescribeStats(*DescribeStats(r))
          }
          result, err = r.Group().finalize(result)
          if err != nil {
            return nil, err
          }
          return result, nil
        }
`,
	}

	defs := MemoExprs{
		Exprs: []ExprDef{
			{
				Name: "hashJoin",
				Join: true,
				Attrs: [][2]string{
					{"innerAttrs", "[]sql.Expression"},
					{"outerAttrs", "[]sql.Expression"},
				},
			},
			{
				Name:       "tableScan",
				SourceType: "*plan.TableNode",
			},
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
