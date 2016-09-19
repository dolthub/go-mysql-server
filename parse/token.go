package parse

type Token struct {
	Type  TokenType
	Value string
	Line  uint
	Pos   uint
}

type TokenType uint

const (
	ErrorToken TokenType = iota
	EOFToken
	LeftParenToken
	RightParenToken
	CommaToken
	DotToken
	KeywordToken
	IdentifierToken
	IntToken
	FloatToken
	StringToken
	OpToken
)

func NewToken(typ TokenType, value string, line, pos uint) *Token {
	return &Token{
		Type:  typ,
		Value: value,
		Line:  line,
		Pos:   pos,
	}
}
