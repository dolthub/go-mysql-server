//go:build cgo && !gms_pure_go

package regex

import regex "github.com/dolthub/go-icu-regex"

type Regex = regex.Regex

var (
	ErrRegexNotYetSet = regex.ErrRegexNotYetSet
	ErrMatchNotYetSet = regex.ErrMatchNotYetSet
	ErrInvalidRegex   = regex.ErrInvalidRegex
)

type RegexFlags = regex.RegexFlags

const (
	RegexFlags_None                     = regex.RegexFlags_None
	RegexFlags_Case_Insensitive         = regex.RegexFlags_Case_Insensitive
	RegexFlags_Comments                 = regex.RegexFlags_Comments
	RegexFlags_Dot_All                  = regex.RegexFlags_Dot_All
	RegexFlags_Literal                  = regex.RegexFlags_Literal
	RegexFlags_Multiline                = regex.RegexFlags_Multiline
	RegexFlags_Unix_Lines               = regex.RegexFlags_Unix_Lines
	RegexFlags_Unicode_Word             = regex.RegexFlags_Unicode_Word
	RegexFlags_Error_On_Unknown_Escapes = regex.RegexFlags_Error_On_Unknown_Escapes
)

func CreateRegex(stringBufferInBytes uint32) Regex {
	return regex.CreateRegex(stringBufferInBytes)
}
