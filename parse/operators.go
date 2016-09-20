package parse

type operator struct {
	name       string
	assoc      Associativity
	precedence uint
}

type Associativity byte

const (
	LeftAssoc Associativity = 1 << iota
	RightAssoc
)

func newOperator(name string, assoc Associativity, precedence uint) *operator {
	return &operator{name, assoc, precedence}
}

func (o *operator) isLeftAssoc() bool {
	return o.assoc == LeftAssoc
}

func (o *operator) isRightAssoc() bool {
	return o.assoc == RightAssoc
}

func (o *operator) comparePrecedence(o2 *operator) int {
	return int(o.precedence) - int(o2.precedence)
}

var opTable = map[string]*operator{
	"-u":   newOperator("-", RightAssoc, 7), // unary minus
	"+":    newOperator("+", LeftAssoc, 6),
	"-":    newOperator("-", LeftAssoc, 6),
	"/":    newOperator("/", LeftAssoc, 6),
	"*":    newOperator("*", LeftAssoc, 6),
	"%":    newOperator("%", LeftAssoc, 6),
	">":    newOperator(">", LeftAssoc, 5),
	">=":   newOperator(">=", LeftAssoc, 5),
	"<":    newOperator("<", LeftAssoc, 5),
	"<=":   newOperator("<=", LeftAssoc, 5),
	"=":    newOperator("=", LeftAssoc, 5),
	"<>":   newOperator("<>", LeftAssoc, 5),
	"like": newOperator("like", LeftAssoc, 5),
	"is":   newOperator("is", LeftAssoc, 5),
	"in":   newOperator("in", LeftAssoc, 5),
	"and":  newOperator("and", LeftAssoc, 4),
	"xor":  newOperator("xor", LeftAssoc, 4),
	"or":   newOperator("or", LeftAssoc, 4),
}
