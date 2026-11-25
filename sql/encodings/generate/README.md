This package generates the embedded binary sort-weight tables and loader code
used by Unicode Collation Algorithm (UCA) collations. Embedding binary tables
keeps builds fast and lets collations load lazily on demand.

Pass `-dsn` (or set `MYSQL_DSN`) to write the UCA 4.0.0 and 5.2.0 tables from
a reference server, and `-collation` to limit that to one collation. Without
`-dsn`, the generator rewrites `weightmaps.go` and the UCA 9.0.0 tables and
leaves the extracted tables untouched.
