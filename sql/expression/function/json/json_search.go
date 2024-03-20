// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package json

import (
	"fmt"
	"gopkg.in/src-d/go-errors.v1"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/jsonpath"
)

// JSONSearch (json_doc, one_or_all, search_str[, escape_char[, path] ...])
//
// JSONSearch Returns the path to the given string within a JSON document. Returns NULL if any of the json_doc,
// search_str, or path arguments are NULL; no path exists within the document; or search_str is not found. An error
// occurs if the json_doc argument is not a valid JSON document, any path argument is not a valid path expression,
// one_or_all is not 'one' or 'all', or escape_char is not a constant expression.
// The one_or_all argument affects the search as follows:
//   - 'one': The search terminates after the first match and returns one path string. It is undefined which match is
//     considered first.
//   - 'all': The search returns all matching path strings such that no duplicate paths are included. If there are
//     multiple strings, they are autowrapped as an array. The order of the array elements is undefined.
//
// Within the search_str search string argument, the % and _ characters work as for the LIKE operator: % matches any
// number of characters (including zero characters), and _ matches exactly one character.
//
// To specify a literal % or _ character in the search string, precede it by the escape character. The default is \ if
// the escape_char argument is missing or NULL. Otherwise, escape_char must be a constant that is empty or one character.
// For more information about matching and escape character behavior, see the description of LIKE in Section 12.8.1,
// “String Comparison Functions and Operators”: https://dev.mysql.com/doc/refman/8.0/en/string-comparison-functions.html
// For escape character handling, a difference from the LIKE behavior is that the escape character for JSON_SEARCH()
// must evaluate to a constant at compile time, not just at execution time. For example, if JSON_SEARCH() is used in a
// prepared statement and the escape_char argument is supplied using a ? parameter, the parameter value might be
// constant at execution time, but is not at compile time.
//
// https://dev.mysql.com/doc/refman/8.0/en/json-search-functions.html#function_json-search
type JSONSearch struct {
	JSON      sql.Expression
	OneOrAll  sql.Expression
	Search    sql.Expression
	Escape    sql.Expression
	Path      sql.Expression
}

var errOneOrAll = fmt.Errorf("the oneOrAll argument to json_search may take these values: 'one' or 'all'")

var _ sql.FunctionExpression = &JSONSearch{}

// NewJSONSearch creates a new NewJSONSearch function.
func NewJSONSearch(args ...sql.Expression) (sql.Expression, error) {
	switch len(args) {
	case 3:
		return &JSONSearch{
			JSON:     args[0],
			OneOrAll: args[1],
			Search:   args[2],
		}, nil
	case 4:
		return &JSONSearch{
			JSON:     args[0],
			OneOrAll: args[1],
			Search:   args[2],
			Escape:   args[3],
		}, nil
	case 5:
		return &JSONSearch{
			JSON:     args[0],
			OneOrAll: args[1],
			Search:   args[2],
			Escape:   args[3],
			Path:     args[4],
		}, nil
	default:
		return nil, sql.ErrInvalidArgumentNumber.New("JSON_SEARCH", "3 to 5", len(args))
	}
}

// FunctionName implements sql.FunctionExpression
func (j *JSONSearch) FunctionName() string {
	return "json_search"
}

// Description implements sql.FunctionExpression
func (j *JSONSearch) Description() string {
	return "path to value within JSON document."
}

// Resolved implements sql.Expression
func (j *JSONSearch) Resolved() bool {
	return j.JSON.Resolved() &&
		j.OneOrAll.Resolved() &&
		j.Search.Resolved() &&
		(j.Escape == nil || j.Escape.Resolved()) &&
		(j.Path == nil || j.Path.Resolved())
}

// String implements sql.Expression
func (j *JSONSearch) String() string {
	// TODO: maybe just don't print if escape/path are nil?
	var escapeStr, pathStr string
	if j.Escape == nil {
		escapeStr = "NULL"
	} else {
		escapeStr = j.Escape.String()
	}
	if j.Path == nil {
		pathStr = "NULL"
	} else {
		pathStr = j.Path.String()
	}
	return fmt.Sprintf("%s(%s, %s, %s, %s, %s)",
		j.FunctionName(),
		j.JSON.String(),
		j.OneOrAll.String(),
		j.Search.String(),
		escapeStr,
		pathStr,
	)
}

// Type implements sql.Expression
func (j *JSONSearch) Type() sql.Type {
	return types.JSON
}

// IsNullable implements sql.Expression
func (j *JSONSearch) IsNullable() bool {
	return j.JSON.IsNullable() ||
		j.OneOrAll.IsNullable() ||
		j.Search.IsNullable() ||
		(j.Escape != nil && j.Escape.IsNullable()) ||
		(j.Path != nil && j.Path.IsNullable())
}

func jsonSearch(json interface{}, searchVal string, currPath string, once bool) ([]string, bool) {
	switch j := json.(type) {
	case string:
		if j == searchVal {
			// Need to format the path as a JSON string
			return []string{fmt.Sprintf(`"%s"`, currPath)}, once
		}
		return nil, false
	case []interface{}:
		var results []string
		for i, v := range j {
			path := fmt.Sprintf("%s[%d]", currPath, i)
			res, stop := jsonSearch(v, searchVal, path, once)
			if stop {
				return res, true
			}
			results = append(results, res...)
		}
		return results, false
	case map[string]interface{}:
		var results []string
		for k, v := range j {
			path := fmt.Sprintf("%s.%s", currPath, k)
			res, stop := jsonSearch(v, searchVal, path, once)
			if stop {
				return res, true
			}
			results = append(results, res...)
		}
		return results, false
	default:
		return nil, false
	}
}

// Eval implements sql.Expression
func (j *JSONSearch) Eval(ctx *sql.Context, row sql.Row) (interface{}, error) {
	span, ctx := ctx.Span(fmt.Sprintf("function.%s", j.FunctionName()))
	defer span.End()

	doc, err := getJSONDocumentFromRow(ctx, row, j.JSON)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, nil
	}

	oneOrAll, err := j.OneOrAll.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if oneOrAll == nil {
		return nil, nil
	}
	oneOrAll, _, err = types.Text.Convert(oneOrAll)
	if err != nil {
		return nil, err
	}
	oneOrAllStr := oneOrAll.(string)
	var isOne bool
	switch strings.ToLower(oneOrAllStr) {
	case "one":
		isOne = true
	case "all":
		isOne = false
	default:
		return nil, errOneOrAll
	}

	search, err := j.Search.Eval(ctx, row)
	if err != nil {
		return nil, err
	}
	if search == nil {
		return nil, nil
	}
	search, _, err = types.Text.Convert(search)
	if err != nil {
		return nil, err
	}
	searchStr := search.(string)

	if j.Escape != nil {
		// TODO: implement escape character handling
	}

	path := "$"
	if j.Path != nil {
		newPath, err := buildPath(ctx, j.Path, row)
		if err != nil {
			return nil, err
		}
		if newPath == nil {
			return nil, nil
		}
		path = newPath.(string)
	}
	js, err := jsonpath.JsonPathLookup(doc.Val, path)
	if err != nil {
		if errors.Is(err, jsonpath.ErrKeyError) {
			return nil, nil
		}
		return nil, err
	}

	res, _ := jsonSearch(js, searchStr, path, isOne)
	if len(res) > 0 && isOne {
		results, _, err := types.JSON.Convert(res[0])
		if err != nil {
			return nil, err
		}
		return results, nil
	}

	results, _, err := types.JSON.Convert(res)
	if err != nil {
		return nil, err
	}
	return results, nil
}

// Children implements sql.Expression
func (j *JSONSearch) Children() []sql.Expression {
	return []sql.Expression{j.JSON, j.OneOrAll, j.Search, j.Escape, j.Path}
}

// WithChildren implements sql.Expression
func (j *JSONSearch) WithChildren(children ...sql.Expression) (sql.Expression, error) {
	return NewJSONSearch(children...)
}