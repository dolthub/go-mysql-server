package parse

import (
	ast "github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/gabereiser/go-mysql-server/sql"
)

//go:generate go run ../../optgen/cmd/optgen/main.go -out window_frame_factory.og.go -pkg parse frameFactory window_frame_factory.go

func getFrameStartNPreceding(ctx *sql.Context, frame *ast.Frame) (sql.Expression, error) {
	if frame == nil || frame.Extent.Start.Type != ast.ExprPreceding {
		return nil, nil
	}
	return ExprToExpression(ctx, frame.Extent.Start.Expr)
}

func getFrameEndNPreceding(ctx *sql.Context, frame *ast.Frame) (sql.Expression, error) {
	if frame == nil || frame.Extent.End == nil || frame.Extent.End.Type != ast.ExprPreceding {
		return nil, nil
	}
	return ExprToExpression(ctx, frame.Extent.End.Expr)
}

func getFrameStartNFollowing(ctx *sql.Context, frame *ast.Frame) (sql.Expression, error) {
	if frame == nil || frame.Extent.Start.Type != ast.ExprFollowing {
		return nil, nil
	}
	return ExprToExpression(ctx, frame.Extent.Start.Expr)
}

func getFrameEndNFollowing(ctx *sql.Context, frame *ast.Frame) (sql.Expression, error) {
	if frame == nil || frame.Extent.End == nil || frame.Extent.End.Type != ast.ExprFollowing {
		return nil, nil
	}
	return ExprToExpression(ctx, frame.Extent.End.Expr)
}

func getFrameStartCurrentRow(ctx *sql.Context, frame *ast.Frame) (bool, error) {
	return frame != nil && frame.Extent.Start.Type == ast.CurrentRow, nil
}

func getFrameEndCurrentRow(ctx *sql.Context, frame *ast.Frame) (bool, error) {
	if frame == nil {
		return false, nil
	}
	if frame.Extent.End == nil {
		// if a frame is not null and only specifies start, default to current row
		return true, nil
	}
	return frame != nil && frame.Extent.End != nil && frame.Extent.End.Type == ast.CurrentRow, nil
}

func getFrameUnboundedPreceding(ctx *sql.Context, frame *ast.Frame) (bool, error) {
	return frame != nil &&
		frame.Extent.Start != nil && frame.Extent.Start.Type == ast.UnboundedPreceding ||
		frame.Extent.End != nil && frame.Extent.End.Type == ast.UnboundedPreceding, nil
}

func getFrameUnboundedFollowing(ctx *sql.Context, frame *ast.Frame) (bool, error) {
	return frame != nil &&
		frame.Extent.Start != nil && frame.Extent.Start.Type == ast.UnboundedFollowing ||
		frame.Extent.End != nil && frame.Extent.End.Type == ast.UnboundedFollowing, nil
}
