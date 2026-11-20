This package generates the embedded binary sort-weight tables and loader code
used by Unicode Collation Algorithm (UCA) collations. Embedding binary tables
keeps builds fast and lets collations load lazily on demand.

Pass `-dsn` (or set `MYSQL_DSN`) to extract the UCA 4.0.0 and 5.2.0 weight
tables directly from a reference server. Ignorable characters receive weight
zero and all other characters receive sequential positive weights.

Running without `-dsn` deduplicates the UCA 9.0.0 maps and regenerates the
loader file while preserving existing extracted UCA 4.0.0 and 5.2.0 binary
tables. The hand-maintained range lookups in the parent package's older UCA
collation files follow the same zero-ignorable convention.
