package parse

type tokenStack []*Token

func (s tokenStack) isEmpty() bool {
	return len(s) == 0
}

func (s tokenStack) peek() *Token {
	if len(s) < 1 {
		return nil
	}
	return s[len(s)-1]
}

func (s *tokenStack) put(t *Token) {
	*s = append(*s, t)
}

func (s *tokenStack) pop() *Token {
	if len(*s) < 1 {
		return nil
	}
	t := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return t
}

func newTokenStack() *tokenStack {
	return new(tokenStack)
}

type stateStack []ParseState

func (s stateStack) isEmpty() bool {
	return len(s) == 0
}

func (s stateStack) peek() ParseState {
	if len(s) < 1 {
		return ErrorState
	}
	return s[len(s)-1]
}

func (s *stateStack) put(st ParseState) {
	*s = append(*s, st)
}

func (s *stateStack) pop() ParseState {
	st := (*s)[len(*s)-1]
	*s = (*s)[:len(*s)-1]
	return st
}

func newStateStack() *stateStack {
	return new(stateStack)
}
