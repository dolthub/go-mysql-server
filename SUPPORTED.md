# Supported SQL Syntax

## Comparisson expressions
- !=
- ==
- \>
- <
- \>=
- <=
- BETWEEN
- IN
- NOT IN
- REGEXP

## Null check expressions
- IS NOT NULL
- IS NULL

## Grouping expressions
- AVG
- COUNT
- MAX
- MIN
- SUM (always returns DOUBLE)

## Standard expressions
- ALIAS (AS)
- CAST/CONVERT
- CREATE TABLE
- DESCRIBE/DESC/EXPLAIN [table name]
- DESCRIBE/DESC/EXPLAIN FORMAT=TREE [query]
- DISTINCT
- FILTER (WHERE)
- GROUP BY
- INSERT INTO
- LIMIT/OFFSET
- LITERAL
- ORDER BY
- SELECT
- SHOW TABLES
- SORT
- STAR (*)

## Index expressions
- CREATE INDEX (an index can be created using either column names or a single arbitrary expression).
- DROP INDEX
- SHOW {INDEXES | INDEX | KEYS} {FROM | IN} [table name]

## Join expressions
- CROSS JOIN
- INNER JOIN
- NATURAL JOIN

## Logical expressions
- AND
- NOT
- OR

## Arithmetic expressions
- \+
- \-
- \*
- \\
- <<
- \>>
- &
- \|
- ^
- div
- %

## Subqueries
- supported only as tables, not as expressions.

## Functions
- ARRAY_LENGTH
- CONCAT
- IS_BINARY
- SPLIT
- SUBSTRING

## Time functions
- DAY
- DAYOFYEAR
- HOUR
- MINUTE
- MONTH
- SECOND
- YEAR
