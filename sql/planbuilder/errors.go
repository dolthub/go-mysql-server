package planbuilder

import (
	"gopkg.in/src-d/go-errors.v1"
	"regexp"
)

var (
	errInvalidDescribeFormat = errors.NewKind("invalid format %q for DESCRIBE, supported formats: %s")

	errInvalidSortOrder = errors.NewKind("invalid sort order: %s")

	ErrPrimaryKeyOnNullField = errors.NewKind("All parts of PRIMARY KEY must be NOT NULL")

	tableCharsetOptionRegex = regexp.MustCompile(`(?i)(DEFAULT)?\s+CHARACTER\s+SET((\s*=?\s*)|\s+)([A-Za-z0-9_]+)`)

	tableCollationOptionRegex = regexp.MustCompile(`(?i)(DEFAULT)?\s+COLLATE((\s*=?\s*)|\s+)([A-Za-z0-9_]+)`)
)
