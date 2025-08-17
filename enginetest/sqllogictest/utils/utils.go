package utils

import (
	"bufio"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

const SEP = "----"

func ReadStmt(scanner *bufio.Scanner) string {
	var stmt string
	once := true
	for {
		if !scanner.Scan() {
			panic("unexpected EOF")
		}
		part := scanner.Text()
		if len(part) == 0 {
			break
		}
		if once {
			once = false
			stmt += part
		} else {
			stmt += "\n" + part
		}
	}
	return stmt
}

// ReadQuery reads queries, stopping at separator
func ReadQuery(scanner *bufio.Scanner) string {
	var query string
	once := true
	for {
		if !scanner.Scan() {
			panic("unexpected EOF")
		}
		part := scanner.Text()
		if len(part) == 0 {
			panic("unexpected blank line")
		}
		if part == SEP {
			break
		}
		if once {
			once = false
			query += part
		} else {
			query += "\n" + part
		}
	}
	return query
}

// ReadResults reads and flattens results, stopping at blank line
func ReadResults(scanner *bufio.Scanner) []string {
	rows := make([]string, 0)
	for {
		if !scanner.Scan() {
			break
		}
		row := scanner.Text()
		if len(row) == 0 {
			break
		}
		rowVals := strings.Fields(row)
		rows = append(rows, rowVals...)
	}
	return rows
}
