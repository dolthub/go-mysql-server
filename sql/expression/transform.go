package expression

import "github.com/src-d/go-mysql-server/sql"

// TransformUp applies a transformation function to the given expression from the
// bottom up.
func TransformUp(e sql.Expression, f sql.TransformExprFunc) (sql.Expression, error) {
	children := e.Children()
	if len(children) == 0 {
		return f(e)
	}

	newChildren := make([]sql.Expression, len(children))
	for i, c := range children {
		c, err := TransformUp(c, f)
		if err != nil {
			return nil, err
		}
		newChildren[i] = c
	}

	e, err := e.WithChildren(newChildren...)
	if err != nil {
		return nil, err
	}

	return f(e)
}
