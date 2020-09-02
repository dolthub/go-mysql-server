package parse

import (
	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"reflect"
	"testing"
)

func TestStringToColumnDefaultValue(t *testing.T) {
	tests := []struct{
		exprStr string
		expectedExpr sql.Expression
	}{
		{
			"2",
			NewColumnDefaultValue(
				expression.NewLiteral(int8(2), sql.Int8),
				nil,
				true,
				true,
			),
		},
		{
			"(2)",
			NewColumnDefaultValue(
				expression.NewLiteral(int8(2), sql.Int8),
				nil,
				false,
				true,
			),
		},
		{
			"(RAND() + 5)",
			NewColumnDefaultValue(
				expression.NewArithmetic(
					expression.NewUnresolvedFunction("rand", false),
					expression.NewLiteral(int8(5), sql.Int8),
					"+",
				),
				nil,
				false,
				true,
			),
		},
		{
			"(GREATEST(RAND(), RAND()))",
			NewColumnDefaultValue(
				expression.NewUnresolvedFunction("greatest", false,
					expression.NewUnresolvedFunction("rand", false),
					expression.NewUnresolvedFunction("rand", false),
				),
				nil,
				false,
				true,
			),
		},
	}

	for _, test := range tests {
		t.Run(test.exprStr, func(t *testing.T) {
			res, err := StringToColumnDefaultValue(sql.NewEmptyContext(), test.exprStr)
			if test.expectedExpr == nil {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, test.expectedExpr, res)
			}
		})
	}
}

// must executes functions of the form "func(args...) (sql.Expression, error)" and panics on errors
func must(f interface{}, args ...interface{}) sql.Expression {
	fType := reflect.TypeOf(f)
	if fType.Kind() != reflect.Func ||
		fType.NumOut() != 2 ||
		!fType.Out(0).AssignableTo(reflect.TypeOf((*sql.Expression)(nil)).Elem()) ||
		!fType.Out(1).AssignableTo(reflect.TypeOf((*error)(nil)).Elem()) {
		panic("invalid function given")
	}
	// we let reflection ensure that the arguments match
	argVals := make([]reflect.Value, len(args))
	for i, arg := range args {
		argVals[i] = reflect.ValueOf(arg)
	}
	fVal := reflect.ValueOf(f)
	out := fVal.Call(argVals)
	err, _ := out[1].Interface().(error)
	if err != nil {
		panic("must err is nil")
	}
	return out[0].Interface().(sql.Expression)
}

func NewColumnDefaultValue(expr sql.Expression, outType sql.Type, representsLiteral bool, mayReturnNil bool) *sql.ColumnDefaultValue {
	cdv, err := sql.NewColumnDefaultValue(expr, outType, representsLiteral, mayReturnNil)
	if err != nil {
		panic(err)
	}
	return cdv
}
