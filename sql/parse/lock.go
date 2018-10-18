package parse

import (
	"bufio"
	"fmt"
	"io"
	"strings"

	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/src-d/go-mysql-server.v0/sql/plan"
)

func parseLockTables(ctx *sql.Context, query string) (sql.Node, error) {
	fmt.Println(query)
	var r = bufio.NewReader(strings.NewReader(query))
	var tables []*plan.TableLock
	err := parseFuncs{
		expect("lock"),
		skipSpaces,
		expect("tables"),
		skipSpaces,
		readTableLocks(&tables),
		skipSpaces,
		checkEOF,
	}.exec(r)

	if err != nil {
		return nil, err
	}

	return plan.NewLockTables(tables), nil
}

func readTableLocks(tables *[]*plan.TableLock) parseFunc {
	return func(rd *bufio.Reader) error {
		for {
			t, err := readTableLock(rd)
			if err != nil {
				return err
			}

			*tables = append(*tables, t)

			if err := skipSpaces(rd); err != nil {
				return err
			}

			b, err := rd.Peek(1)
			if err == io.EOF {
				return nil
			} else if err != nil {
				return err
			}

			if string(b) != "," {
				return nil
			}

			if _, err := rd.Discard(1); err != nil {
				return err
			}

			if err := skipSpaces(rd); err != nil {
				return err
			}
		}
	}
}

func readTableLock(rd *bufio.Reader) (*plan.TableLock, error) {
	var tableName string
	var write bool

	err := parseFuncs{
		readQuotableIdent(&tableName),
		skipSpaces,
		maybeReadAlias,
		skipSpaces,
		readLockType(&write),
	}.exec(rd)
	if err != nil {
		return nil, err
	}

	return &plan.TableLock{
		Table: plan.NewUnresolvedTable(tableName, ""),
		Write: write,
	}, nil
}

func maybeReadAlias(rd *bufio.Reader) error {
	data, err := rd.Peek(2)
	if err != nil {
		return err
	}

	if strings.ToLower(string(data)) == "as" {
		_, err := rd.Discard(2)
		if err != nil {
			return err
		}

		if err := skipSpaces(rd); err != nil {
			return err
		}

		var ignored string
		if err := readIdent(&ignored)(rd); err != nil {
			return err
		}

		return nil
	}

	var nextIdent string
	if err := readIdent(&nextIdent)(rd); err != nil {
		return err
	}

	switch strings.ToLower(nextIdent) {
	case "read", "low_priority", "write":
		unreadString(rd, nextIdent)
	}

	return nil
}

func readLockType(write *bool) parseFunc {
	return func(rd *bufio.Reader) error {
		var nextIdent string
		if err := readIdent(&nextIdent)(rd); err != nil {
			return err
		}

		switch strings.ToLower(nextIdent) {
		case "low_priority":
			err := parseFuncs{skipSpaces, expect("write")}.exec(rd)
			if err != nil {
				return err
			}

			fallthrough
		case "write":
			*write = true
			return nil
		case "read":
			var ident string
			if err := skipSpaces(rd); err != nil {
				return err
			}

			if err := readIdent(&ident)(rd); err != nil {
				return err
			}

			if ident != "" && ident != "local" {
				return errUnexpectedSyntax.New("LOCAL", ident)
			}

			return nil
		default:
			return errUnexpectedSyntax.New("one of: READ, WRITE or LOW_PRIORITY", nextIdent)
		}
	}
}
