package parse

type stack []*Token

func (s stack) isEmpty() bool {
	return len(s) == 0
}

func (s stack) peek() *Token {
	return s[len(s)-1]
}

func (s *stack) put(t *Token) {
	*s = append(*s, t)
}

func (s *stack) pop() *Token {
	t := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return t
}

func newStack() *stack {
	return new(stack)
}
