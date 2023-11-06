package stats

import "github.com/dolthub/go-mysql-server/sql"

func Union(s1, s2 sql.Statistic) sql.Statistic {
	return nil
}

func Intersect(s1, s2 sql.Statistic) sql.Statistic {
	return nil
}

func PrefixKey(s1 sql.Statistic, key []interface{}, nullable []bool) sql.Statistic {
	return nil
}

func PrefixLt(s1 sql.Statistic, val interface{}) sql.Statistic {
	return nil
}

func PrefixGt(s1 sql.Statistic, val interface{}) sql.Statistic {
	return nil
}

func PrefixLte(s1 sql.Statistic, val interface{}) sql.Statistic {
	return nil
}

func PrefixGte(s1 sql.Statistic, val interface{}) sql.Statistic {
	return nil
}

func PrefixIsNull(s1 sql.Statistic, val interface{}) sql.Statistic {
	return nil
}

func PrefixIsNotNull(s1 sql.Statistic, val interface{}) sql.Statistic {
	return nil
}

func McvIndexGt(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return nil
}

func McvIndexLt(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return nil
}

func McvIndexGte(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return nil
}

func McvIndexLte(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return nil
}

func McvIndexIsNull(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return nil
}

func McvIndexIsNotNull(s sql.Statistic, i int, val interface{}) sql.Statistic {
	return nil
}
