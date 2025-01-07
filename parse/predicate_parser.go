package parse

type PredParser struct {
	lex *Lexer
}

func NewPredParser(s string) *PredParser {
	return &PredParser{lex: NewLexer(s)}
}

func (pp *PredParser) field() (string, error) {
	return pp.lex.EatId()
}

func (pp *PredParser) constant() error {
	if pp.lex.MatchStringConstant() {
		_, err := pp.lex.EatStringConstant()
		return err
	} else {
		_, err := pp.lex.EatIntConstant()
		return err
	}
}

func (pp *PredParser) expression() error {
	if pp.lex.MatchId() {
		_, err := pp.field()
		return err
	} else {
		return pp.constant()
	}
}

func (pp *PredParser) term() error {
	// expression
	if err := pp.expression(); err != nil {
		return err
	}
	// eat '='
	if err := pp.lex.EatDelim('='); err != nil {
		return err
	}
	// next expression
	if err := pp.expression(); err != nil {
		return err
	}
	return nil
}

func (pp *PredParser) predicate() error {
	if err := pp.term(); err != nil {
		return err
	}
	if pp.lex.MatchKeyword("and") {
		// eat "and"
		if err := pp.lex.EatKeyword("and"); err != nil {
			return err
		}
		return pp.predicate()
	}
	return nil
}
