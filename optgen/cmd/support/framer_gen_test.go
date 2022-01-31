package support

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestFramerGen(t *testing.T) {
	test := struct {
		expected string
	}{
		expected: `
		import (
		  "github.com/dolthub/go-mysql-server/sql"
		  "github.com/dolthub/go-mysql-server/sql/expression"
		)
		
		type RowsUnboundedPrecedingToNPrecedingFramer struct {
		  rowFramerBase
		}
		
		var _ sql.WindowFramer = (*RowsUnboundedPrecedingToNPrecedingFramer)(nil)
		
		func NewRowsUnboundedPrecedingToNPrecedingFramer(frame sql.WindowFrame, window *sql.Window) (sql.WindowFramer, error) {
		  unboundedPreceding := true
		  endNPreceding, err := expression.LiteralToInt(frame.EndNPreceding())
		  if err != nil {
			return nil, err
		  }
		  return &RowsUnboundedPrecedingToNPrecedingFramer{
			rowFramerBase{
			  unboundedPreceding: unboundedPreceding,
			  endNPreceding: endNPreceding,
			},
		  }, nil
		}
		`,
	}

	gen := FramerGen{limit: 1}
	var buf bytes.Buffer
	gen.Generate(nil, &buf)

	if testing.Verbose() {
		fmt.Printf("\n=>\n\n%s\n", buf.String())
	}

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(test.expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", test.expected, buf.String())
	}
}
