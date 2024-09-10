package rdparser

import (
	"context"
	"testing"

	ast "github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/stretchr/testify/require"
)

func TestParser(t *testing.T) {
	tests := []struct {
		q   string
		exp ast.Statement
		ok  bool
	}{
		{
			q:  "insert into xy values (0,'0', .0), (1,'1', 1.0)",
			ok: true,
		},
		{
			q:  "insert into xy (x,y,z) values (0,'0', .0), (1,'1', 1.0)",
			ok: true,
		},
		{
			q:  "insert into db.xy values (0,'0', .0), (1,'1', 1.0)",
			ok: true,
		},
		{
			q:  "select * from xy where x = 1",
			ok: true,
		},
		{
			q:  "select id from sbtest1 where id = 1000",
			ok: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.q, func(t *testing.T) {
			p := new(parser)
			p.tok = ast.NewStringTokenizer(tt.q)
			res, ok := p.statement(context.Background())
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.exp, res)
		})
	}
}
