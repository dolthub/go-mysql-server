package function

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/oliveagle/jsonpath"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
)

// JSONExtract extracts data from a json document using json paths.
type JSONExtract struct {
	JSON  sql.Expression
	Paths []sql.Expression
}

// NewJSONExtract creates a new JSONExtract UDF.
func NewJSONExtract(args ...sql.Expression) (sql.Expression, error) {
	if len(args) < 2 {
		return nil, sql.ErrInvalidArgumentNumber.New(2, len(args))
	}

	return &JSONExtract{args[0], args[1:]}, nil
}

// Resolved implements the sql.Expression interface.
func (j *JSONExtract) Resolved() bool {
	for _, p := range j.Paths {
		if !p.Resolved() {
			return false
		}
	}
	return j.JSON.Resolved()
}

// Type implements the sql.Expression interface.
func (j *JSONExtract) Type() sql.Type { return sql.JSON }

// Eval implements the sql.Expression interface.
func (j *JSONExtract) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span("function.JSONExtract")
	defer span.Finish()

	js, err := j.JSON.Eval(ctx, row)
	if err != nil {
		return nil, err
	}

	js, err = sql.JSON.Convert(js)
	if err != nil {
		return nil, err
	}

	var doc interface{}
	if err := json.Unmarshal(js.([]byte), &doc); err != nil {
		return nil, err
	}

	var result = make([]interface{}, len(j.Paths))
	for i, p := range j.Paths {
		path, err := p.Eval(ctx, row)
		if err != nil {
			return nil, err
		}

		path, err = sql.Text.Convert(path)
		if err != nil {
			return nil, err
		}

		result[i], err = jsonpath.JsonPathLookup(doc, path.(string))
		if err != nil {
			return nil, err
		}
	}

	if len(result) == 1 {
		return result[0], nil
	}

	return result, nil
}

// IsNullable implements the sql.Expression interface.
func (j *JSONExtract) IsNullable() bool {
	for _, p := range j.Paths {
		if p.IsNullable() {
			return true
		}
	}
	return j.JSON.IsNullable()
}

// Children implements the sql.Expression interface.
func (j *JSONExtract) Children() []sql.Expression {
	return append([]sql.Expression{j.JSON}, j.Paths...)
}

// TransformUp implements the sql.Expression interface.
func (j *JSONExtract) TransformUp(f sql.TransformExprFunc) (sql.Expression, error) {
	json, err := j.JSON.TransformUp(f)
	if err != nil {
		return nil, err
	}

	paths := make([]sql.Expression, len(j.Paths))
	for i, p := range j.Paths {
		paths[i], err = p.TransformUp(f)
		if err != nil {
			return nil, err
		}
	}

	return f(&JSONExtract{json, paths})
}

func (j *JSONExtract) String() string {
	children := j.Children()
	var parts = make([]string, len(children))
	for i, c := range children {
		parts[i] = c.String()
	}
	return fmt.Sprintf("JSON_EXTRACT(%s)", strings.Join(parts, ", "))
}
