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
- COUNT and COUNT(DISTINCT)
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
- SHOW PROCESSLIST
- SHOW TABLE STATUS
- SHOW VARIABLES
- SHOW CREATE DATABASE
- SHOW CREATE TABLE
- SHOW FIELDS FROM
- LOCK/UNLOCK
- USE
- SHOW DATABASES
- SHOW WARNINGS
- INTERVALS

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
- \+ (including between dates and intervals)
- \- (including between dates and intervals)
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
- CEIL
- CEILING
- COALESCE
- CONCAT
- CONCAT_WS
- CONNECTION_ID
- DATABASE
- FLOOR
- FROM_BASE64
- GREATEST
- IS_BINARY
- IS_BINARY
- JSON_EXTRACT
- JSON_UNQUOTE
- LEAST
- LN
- LOG10
- LOG2
- LOWER
- LPAD
- POW
- POWER
- ROUND
- RPAD
- SLEEP
- SOUNDEX
- SPLIT
- SQRT
- SUBSTRING
- TO_BASE64
- UPPER

## Time functions
- DATE
- DATE_ADD
- DATE_SUB
- DAY
- DAYOFMONTH
- DAYOFWEEK
- DAYOFYEAR
- HOUR
- MINUTE
- MONTH
- NOW
- SECOND
- WEEKDAY
- YEAR
- YEARWEEK
