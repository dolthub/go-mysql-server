package planbuilder

import (
	"testing"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/types"
)

func TestWindowFrameGetters(t *testing.T) {
	exprTests := []struct {
		Name     string
		Fn       func(*Builder, *scope, *ast.Frame) sql.Expression
		Frame    *ast.Frame
		Expected sql.Expression
	}{
		{
			Name: "start preceding int",
			Fn:   (*Builder).getFrameStartNPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{
						Expr: ast.NewIntVal([]byte("1")),
						Type: ast.ExprPreceding,
					},
				},
			},
			Expected: expression.NewLiteral(int8(1), types.Int8),
		},
		{
			Name: "start preceding nil",
			Fn:   (*Builder).getFrameStartNPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{},
				},
			},
			Expected: nil,
		},
		{
			Name: "end preceding int",
			Fn:   (*Builder).getFrameEndNPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{
						Expr: ast.NewIntVal([]byte("1")),
						Type: ast.ExprPreceding,
					},
				},
			},
			Expected: expression.NewLiteral(int8(1), types.Int8),
		},
		{
			Name: "end preceding nil",
			Fn:   (*Builder).getFrameEndNPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{},
				},
			},
			Expected: nil,
		},
		{
			Name: "start following int",
			Fn:   (*Builder).getFrameStartNFollowing,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{
						Expr: ast.NewIntVal([]byte("1")),
						Type: ast.ExprFollowing,
					},
				},
			},
			Expected: expression.NewLiteral(int8(1), types.Int8),
		},
		{
			Name: "start following nil",
			Fn:   (*Builder).getFrameStartNFollowing,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{},
				},
			},
			Expected: nil,
		},
		{
			Name: "end following int",
			Fn:   (*Builder).getFrameEndNFollowing,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{
						Expr: ast.NewIntVal([]byte("1")),
						Type: ast.ExprFollowing,
					},
				},
			},
			Expected: expression.NewLiteral(int8(1), types.Int8),
		},
		{
			Name: "end following nil",
			Fn:   (*Builder).getFrameEndNFollowing,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{},
				},
			},
			Expected: nil,
		},
	}

	boolTests := []struct {
		Name     string
		Fn       func(*Builder, *scope, *ast.Frame) bool
		Frame    *ast.Frame
		Expected bool
	}{
		{
			Name: "start current row is set",
			Fn:   (*Builder).getFrameStartCurrentRow,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{
						Type: ast.CurrentRow,
					},
				},
			},
			Expected: true,
		},
		{
			Name: "start current row is not set",
			Fn:   (*Builder).getFrameStartCurrentRow,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{
						Type: -1,
					},
				},
			},
			Expected: false,
		},
		{
			Name: "end current row is set",
			Fn:   (*Builder).getFrameEndCurrentRow,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{
						Type: ast.CurrentRow,
					},
				},
			},
			Expected: true,
		},
		{
			Name: "end current row is not set",
			Fn:   (*Builder).getFrameEndCurrentRow,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{
						Type: -1,
					},
				},
			},
			Expected: false,
		},
		{
			Name: "unbounded preceding is set",
			Fn:   (*Builder).getFrameUnboundedPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{
						Type: ast.UnboundedPreceding,
					},
				},
			},
			Expected: true,
		},
		{
			Name: "unbounded preceding is not set",
			Fn:   (*Builder).getFrameUnboundedPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{},
				},
			},
			Expected: false,
		},
		{
			Name: "unbounded following is set",
			Fn:   (*Builder).getFrameUnboundedFollowing,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{
						Type: ast.UnboundedFollowing,
					},
				},
			},
			Expected: true,
		},
		{
			Name: "unbounded following is not set",
			Fn:   (*Builder).getFrameUnboundedFollowing,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{},
				},
			},
			Expected: false,
		},
	}

	for _, tt := range exprTests {
		t.Run(tt.Name, func(t *testing.T) {
			b := &Builder{}
			res := tt.Fn(b, &scope{b: b}, tt.Frame)
			require.Equal(t, tt.Expected, res)
		})
	}

	for _, tt := range boolTests {
		t.Run(tt.Name, func(t *testing.T) {
			b := &Builder{}
			res := tt.Fn(b, &scope{b: b}, tt.Frame)
			require.Equal(t, tt.Expected, res)
		})
	}
}
