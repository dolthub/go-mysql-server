package support

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestFrameFactoryGen(t *testing.T) {
	test := struct {
		expected string
	}{
		expected: `
		import (
		  "fmt"
		  "github.com/gabereiser/go-mysql-server/sql"
		  "github.com/gabereiser/go-mysql-server/sql/plan"
		  ast "github.com/dolthub/vitess/go/vt/sqlparser"
		)
		
		func NewFrame(ctx *sql.Context, f *ast.Frame) (sql.WindowFrame, error) {
		  if f == nil {
			return nil, nil
		  }
		  isRange := f.Unit == ast.RangeUnit
		  isRows := f.Unit == ast.RowsUnit
		  unboundedPreceding, err := getFrameUnboundedPreceding(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  startNPreceding, err := getFrameStartNPreceding(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  startCurrentRow, err := getFrameStartCurrentRow(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  startNFollowing, err := getFrameStartNFollowing(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  endNPreceding, err := getFrameEndNPreceding(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  endCurrentRow, err := getFrameEndCurrentRow(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  endNFollowing, err := getFrameEndNFollowing(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  unboundedFollowing, err := getFrameUnboundedFollowing(ctx, f)
		  if err != nil {
			return nil, err
		  }
		  switch {
		  case isRows && unboundedPreceding && endNPreceding != nil:
			return plan.NewRowsUnboundedPrecedingToNPrecedingFrame(endNPreceding), nil
		  }
		  return nil, fmt.Errorf("no matching constructor found for frame: %v", f)
		}
		`,
	}

	gen := FrameFactoryGen{limit: 1}
	var buf bytes.Buffer
	gen.Generate(nil, &buf)

	if testing.Verbose() {
		fmt.Printf("\n=>\n\n%s\n", buf.String())
	}

	if !strings.Contains(removeWhitespace(buf.String()), removeWhitespace(test.expected)) {
		t.Fatalf("\nexpected:\n%s\nactual:\n%s", test.expected, buf.String())
	}
}
