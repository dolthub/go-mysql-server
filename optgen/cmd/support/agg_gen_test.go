package support

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestAggGen(t *testing.T) {
	test := struct {
		defines  AggDefs
		expected string
	}{
		defines: AggDefs{
			[]AggDef{
				{
					Name:          "Test",
					Desc:          "Test description",
					RetType:       "sql.Float64",
					SqlString:     true,
					WindowSqlName: "test_window",
				},
			},
		},
		expected: `
         import (
            "fmt"
            "github.com/dolthub/go-mysql-server/sql/types"
            "github.com/dolthub/go-mysql-server/sql"
            "github.com/dolthub/go-mysql-server/sql/expression"
            "github.com/dolthub/go-mysql-server/sql/transform"
        )

        type Test struct{
            unaryAggBase
        }

        var _ sql.FunctionExpression = (*Test)(nil)
        var _ sql.Aggregation = (*Test)(nil)
        var _ sql.Describable = (*Test)(nil)
        var _ sql.WindowAdaptableExpression = (*Test)(nil)

        func NewTest(e sql.Expression) *Test {
            return &Test{
                unaryAggBase{
                    Child: e,
                    functionName: "Test",
                    description: "Test description",
                },
            }
        }

        func (a *Test) Type(ctx *sql.Context) sql.Type {
            return sql.Float64
        }

        func (a *Test) IsNullable(ctx *sql.Context) bool {
            return false
        }

        func (a *Test) String() string {
          if a.window != nil {
	        return "TEST_WINDOW(" + a.Child.String() + ") " + a.window.String()
          }
	      return "TEST(" + a.Child.String() + ")"
        }

        func (a *Test) Describe(ctx *sql.Context, options sql.DescribeOptions) string {
          if options.Debug {
            if a.window != nil {
              pr := sql.NewTreePrinter()
              _ = pr.WriteNode("TEST")
	      children := []string{sql.Describe(ctx, a.window, options), sql.Describe(ctx, a.Child, options)}
              pr.WriteChildren(children...)
              return pr.String()
            }
            return fmt.Sprintf("TEST(%s)", sql.Describe(ctx, a.Child, options))
          }
          if a.window != nil {
            return "TEST_WINDOW(" + sql.Describe(ctx, a.Child, options) + ") " + sql.Describe(ctx, a.window, options)
          }
          return "TEST(" + sql.Describe(ctx, a.Child, options) + ")"
        }

        func (a *Test) DebugString(ctx *sql.Context) string {
          return a.Describe(ctx, sql.DescribeOptions{Debug: true})
        }

        func (a *Test) WithWindow(ctx *sql.Context, window *sql.WindowDefinition) sql.WindowAdaptableExpression {
            res := a.unaryAggBase.WithWindow(ctx, window)
            return &Test{unaryAggBase: *res.(*unaryAggBase)}
        }

        func (a *Test) WithChildren(ctx *sql.Context, children ...sql.Expression) (sql.Expression, error) {
            res, err := a.unaryAggBase.WithChildren(ctx, children...)
            return &Test{unaryAggBase: *res.(*unaryAggBase)}, err
        }

        func (a *Test) WithId(id sql.ColumnId) sql.IdExpression {
            res := a.unaryAggBase.WithId(id)
            return &Test{unaryAggBase: *res.(*unaryAggBase)}
        }

        func (a *Test) NewBuffer(ctx *sql.Context) (sql.AggregationBuffer, error) {
            child, err := transform.Clone(ctx, a.Child)
            if err != nil {
                return nil, err
            }
            return NewTestBuffer(child), nil
        }

        func (a *Test) NewWindowFunction(ctx *sql.Context) (sql.WindowFunction, error) {
            child, err := transform.Clone(ctx, a.Child)
            if err != nil {
                return nil, err
            }
            return NewTestAgg(child).WithWindow(ctx, a.Window())
        }
		`,
	}

	var gen AggGen
	var buf bytes.Buffer
	gen.Generate(test.defines, &buf)

	if testing.Verbose() {
		fmt.Printf("%+v\n=>\n\n%s\n", test.defines, buf.String())
	}

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(test.expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", test.expected, buf.String())
	}
}

func removeWhitespace(s string) string {
	return strings.Trim(strings.Replace(strings.Replace(s, " ", "", -1), "\t", "", -1), " \t\r\n")
}
