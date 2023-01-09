package parse

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
		Fn       func(ctx *sql.Context, frame *ast.Frame) (sql.Expression, error)
		Frame    *ast.Frame
		Expected sql.Expression
	}{
		{
			Name: "start preceding int",
			Fn:   getFrameStartNPreceding,
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
			Fn:   getFrameStartNPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{},
				},
			},
			Expected: nil,
		},
		{
			Name: "end preceding int",
			Fn:   getFrameEndNPreceding,
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
			Fn:   getFrameEndNPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					End: &ast.FrameBound{},
				},
			},
			Expected: nil,
		},
		{
			Name: "start following int",
			Fn:   getFrameStartNFollowing,
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
			Fn:   getFrameStartNFollowing,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{},
				},
			},
			Expected: nil,
		},
		{
			Name: "end following int",
			Fn:   getFrameEndNFollowing,
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
			Fn:   getFrameEndNFollowing,
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
		Fn       func(ctx *sql.Context, frame *ast.Frame) (bool, error)
		Frame    *ast.Frame
		Expected bool
	}{
		{
			Name: "start current row is set",
			Fn:   getFrameStartCurrentRow,
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
			Fn:   getFrameStartCurrentRow,
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
			Fn:   getFrameEndCurrentRow,
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
			Fn:   getFrameEndCurrentRow,
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
			Fn:   getFrameUnboundedPreceding,
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
			Fn:   getFrameUnboundedPreceding,
			Frame: &ast.Frame{
				Extent: &ast.FrameExtent{
					Start: &ast.FrameBound{},
				},
			},
			Expected: false,
		},
		{
			Name: "unbounded following is set",
			Fn:   getFrameUnboundedFollowing,
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
			Fn:   getFrameUnboundedFollowing,
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
			ctx := sql.NewEmptyContext()
			res, err := tt.Fn(ctx, tt.Frame)
			require.NoError(t, err)
			require.Equal(t, tt.Expected, res)
		})
	}

	for _, tt := range boolTests {
		t.Run(tt.Name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			res, err := tt.Fn(ctx, tt.Frame)
			require.NoError(t, err)
			require.Equal(t, tt.Expected, res)
		})
	}
}
