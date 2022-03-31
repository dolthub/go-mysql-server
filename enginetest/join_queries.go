package enginetest

import (
	"github.com/dolthub/go-mysql-server/sql"
)

var JoinQueryTests = []QueryTest{
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v2 where b.pk = 0 and a.v2 = 1;",
		Expected: []sql.Row{
			{2, 0},
			{2, 0},
			{2, 0},
			{2, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v3 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b inner join (select * from one_pk_three_idx where v2 = 0) c on b.pk = c.v3 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b left join one_pk_three_idx c on b.pk = c.v1+1 where b.pk = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, nil},
			{1, nil},
			{2, nil},
			{3, nil},
			{4, nil},
			{5, nil},
			{6, nil},
			{7, nil},
		},
	},
	{
		Query: "select a.pk, c.v2 from one_pk_three_idx a cross join one_pk_three_idx b right join one_pk_three_idx c on b.pk = c.v1 where b.pk = 0 and c.v2 = 0 order by a.pk;",
		Expected: []sql.Row{
			{0, 0},
			{0, 0},
			{1, 0},
			{1, 0},
			{2, 0},
			{2, 0},
			{3, 0},
			{3, 0},
			{4, 0},
			{4, 0},
			{5, 0},
			{5, 0},
			{6, 0},
			{6, 0},
			{7, 0},
			{7, 0},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b RIGHT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{1, "first row", 2, "second row", 1, "first row"},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 2, "second row", 1, "first row"},
			{1, "first row", 3, "third row", 2, "second row"},
			{2, "second row", 3, "third row", 2, "second row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{nil, nil, nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b LEFT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{3, "third row", 1, "first row", nil, nil},
			{2, "second row", 1, "first row", nil, nil},
			{1, "first row", 1, "first row", nil, nil},
			{3, "third row", 2, "second row", 1, "first row"},
			{2, "second row", 2, "second row", 1, "first row"},
			{1, "first row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{2, "second row", 3, "third row", 2, "second row"},
			{1, "first row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a CROSS JOIN mytable b LEFT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{3, "third row", 1, "first row", 2, "second row"},
			{2, "second row", 1, "first row", 2, "second row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{3, "third row", 2, "second row", 3, "third row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{1, "first row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
			{2, "second row", 3, "third row", nil, nil},
			{1, "first row", 3, "third row", nil, nil},
		}},
	{
		Query: "select * from mytable a LEFT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", nil, nil},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a LEFT JOIN  mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{nil, nil, nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
			{nil, nil, nil, nil, 3, "third row"},
		},
	},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i = c.i + 1;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", nil, nil},
			{2, "second row", 2, "second row", 1, "first row"},
			{3, "third row", 3, "third row", 2, "second row"},
		},
	},
	{
		Query: "select * from mytable a LEFT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
		}},
	{
		Query: "select * from mytable a LEFT JOIN  mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
		}},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i RIGHT JOIN mytable c ON b.i+1= c.i;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
		}},
	{
		Query: "select * from mytable a RIGHT JOIN mytable b on a.i = b.i LEFT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 3, "third row", nil, nil},
		},
	},
	{
		// todo: CROSS JOIN -> RIGHT JOIN correctness
		Query: "select * from mytable a CROSS JOIN mytable b RIGHT JOIN mytable c ON b.i+1 = c.i;",
		Expected: []sql.Row{
			{nil, nil, nil, nil, 1, "first row"},
			{1, "first row", 1, "first row", 2, "second row"},
			{2, "second row", 1, "first row", 2, "second row"},
			{3, "third row", 1, "first row", 2, "second row"},
			{1, "first row", 2, "second row", 3, "third row"},
			{2, "second row", 2, "second row", 3, "third row"},
			{3, "third row", 2, "second row", 3, "third row"},
		},
	},
}
