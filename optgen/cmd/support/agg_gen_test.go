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
					Name:    "Test",
					Desc:    "Test description",
					RetType: "sql.Float64",
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
        var _ sql.WindowAdaptableExpression = (*Test)(nil)

        func NewTest(e sql.Expression) *Test {
            return &Test{
                unaryAggBase{
                    UnaryExpression: expression.UnaryExpression{Child: e},
                    functionName: "Test",
                    description: "Test description",
                },
            }
        }

        func (a *Test) Type() sql.Type {
            return sql.Float64
        }

        func (a *Test) IsNullable() bool {
            return false
        }

        func (a *Test) String() string {
          if a.window != nil {
            pr := sql.NewTreePrinter()
            _ = pr.WriteNode("TEST")
        	    children := []string{a.window.String(), a.Child.String()}
            pr.WriteChildren(children...)
            return pr.String()
          }
          return fmt.Sprintf("TEST(%s)", a.Child)
        }

        func (a *Test) DebugString() string {
          if a.window != nil {
            pr := sql.NewTreePrinter()
            _ = pr.WriteNode("TEST")
        	    children := []string{sql.DebugString(a.window), sql.DebugString(a.Child)}
            pr.WriteChildren(children...)
            return pr.String()
          }
          return fmt.Sprintf("TEST(%s)", sql.DebugString(a.Child))
        }

        func (a *Test) WithWindow(window *sql.WindowDefinition) (sql.Aggregation, error) {
            res, err := a.unaryAggBase.WithWindow(window)
            return &Test{unaryAggBase: *res.(*unaryAggBase)}, err
        }

        func (a *Test) WithChildren(children ...sql.Expression) (sql.Expression, error) {
            res, err := a.unaryAggBase.WithChildren(children...)
            return &Test{unaryAggBase: *res.(*unaryAggBase)}, err
        }

        func (a *Test) NewBuffer() (sql.AggregationBuffer, error) {
            child, err := transform.Clone(a.Child)
            if err != nil {
                return nil, err
            }
            return NewTestBuffer(child), nil
        }

        func (a *Test) NewWindowFunction() (sql.WindowFunction, error) {
            child, err := transform.Clone(a.Child)
            if err != nil {
                return nil, err
            }
            return NewTestAgg(child).WithWindow(a.Window())
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
