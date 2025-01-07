// Updated lexer with corrected date parsing logic
package parse

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// SyntaxError indicates invalid syntax encountered by the lexer.
type SyntaxError struct {
	Message string
}

func (e *SyntaxError) Error() string {
	return fmt.Sprintf("syntax error: %s", e.Message)
}

// TokenType enumerates all recognized token types.
type TokenType int

const (
	TTDelimiter TokenType = iota
	TTNumber
	TTString
	TTWord
	TTBoolean
	TTDate
	TTOperator
	TTEOF
)

// Token holds data about a single token.
type Token struct {
	Type      TokenType
	StringVal string    // for string/word tokens or operator text
	NumVal    int       // for integer tokens
	BoolVal   bool      // for boolean tokens
	TimeVal   time.Time // for date tokens
	Rune      rune      // for delimiter tokens (e.g. ',', '(', ')', ...)
}

// Lexer processes an input string and produces tokens on demand.
type Lexer struct {
	input        string
	position     int
	currentToken Token
	keywords     map[string]struct{}
}

// NewLexer creates a new Lexer from the given SQL statement.
func NewLexer(s string) *Lexer {
	l := &Lexer{input: s}
	l.initKeywords()
	_ = l.nextToken() // Get the first token (ignore error here, or handle it)
	return l
}

//----------------------------
// Public "match" methods
//----------------------------

// MatchDelim returns true if the current token is the specified delimiter.
func (l *Lexer) MatchDelim(d rune) bool {
	return l.currentToken.Type == TTDelimiter && l.currentToken.Rune == d
}

// MatchIntConstant returns true if the current token is an integer.
func (l *Lexer) MatchIntConstant() bool {
	return l.currentToken.Type == TTNumber
}

// MatchStringConstant returns true if the current token is a string constant.
func (l *Lexer) MatchStringConstant() bool {
	return l.currentToken.Type == TTString
}

// MatchKeyword returns true if the current token is the specified keyword.
func (l *Lexer) MatchKeyword(w string) bool {
	return l.currentToken.Type == TTWord && l.currentToken.StringVal == strings.ToLower(w)
}

// MatchId returns true if the current token is a legal identifier (non-keyword).
func (l *Lexer) MatchId() bool {
	if l.currentToken.Type != TTWord {
		return false
	}
	_, isKeyword := l.keywords[l.currentToken.StringVal]
	return !isKeyword
}

// MatchBooleanConstant returns true if the current token is a boolean (true/false).
func (l *Lexer) MatchBooleanConstant() bool {
	return l.currentToken.Type == TTBoolean
}

// MatchDateConstant returns true if the current token is a date token.
func (l *Lexer) MatchDateConstant() bool {
	return l.currentToken.Type == TTDate
}

// MatchOperator returns true if the current token is an operator (e.g. "=", ">=", etc.).
func (l *Lexer) MatchOperator(op string) bool {
	return l.currentToken.Type == TTOperator && l.currentToken.StringVal == op
}

//----------------------------
// Public "eat" methods
//----------------------------

func (l *Lexer) EatDelim(d rune) error {
	if !l.MatchDelim(d) {
		return &SyntaxError{Message: fmt.Sprintf("expected delimiter '%c'", d)}
	}
	return l.nextToken()
}

func (l *Lexer) EatIntConstant() (int, error) {
	if !l.MatchIntConstant() {
		return 0, &SyntaxError{Message: "expected integer constant"}
	}
	val := l.currentToken.NumVal
	if err := l.nextToken(); err != nil {
		return 0, err
	}
	return val, nil
}

func (l *Lexer) EatStringConstant() (string, error) {
	if !l.MatchStringConstant() {
		return "", &SyntaxError{Message: "expected string constant"}
	}
	val := l.currentToken.StringVal
	if err := l.nextToken(); err != nil {
		return "", err
	}
	return val, nil
}

func (l *Lexer) EatKeyword(w string) error {
	if !l.MatchKeyword(w) {
		return &SyntaxError{Message: fmt.Sprintf("expected keyword '%s'", w)}
	}
	return l.nextToken()
}

func (l *Lexer) EatId() (string, error) {
	if !l.MatchId() {
		return "", &SyntaxError{Message: "expected identifier"}
	}
	val := l.currentToken.StringVal
	if err := l.nextToken(); err != nil {
		return "", err
	}
	return val, nil
}

func (l *Lexer) EatBooleanConstant() (bool, error) {
	if !l.MatchBooleanConstant() {
		return false, &SyntaxError{Message: "expected boolean constant (true/false)"}
	}
	val := l.currentToken.BoolVal
	if err := l.nextToken(); err != nil {
		return false, err
	}
	return val, nil
}

func (l *Lexer) EatDateConstant() (time.Time, error) {
	if !l.MatchDateConstant() {
		return time.Time{}, &SyntaxError{Message: "expected date constant"}
	}
	val := l.currentToken.TimeVal
	if err := l.nextToken(); err != nil {
		return time.Time{}, err
	}
	return val, nil
}

// EatOperator checks if the current token is the specified operator.
// If so, it advances the lexer; otherwise returns an error.
func (l *Lexer) EatOperator(op string) error {
	if !l.MatchOperator(op) {
		return &SyntaxError{Message: fmt.Sprintf("expected operator '%s'", op)}
	}
	return l.nextToken()
}

//----------------------------
// Private methods
//----------------------------

// nextToken advances to the next token and sets l.currentToken accordingly.
// It returns an error if there's an unexpected character or parse failure.
func (l *Lexer) nextToken() error {
	l.skipWhitespace()
	if l.position >= len(l.input) {
		l.currentToken = Token{Type: TTEOF}
		return nil // end of input is not an error
	}

	r, width := utf8.DecodeRuneInString(l.input[l.position:])

	// Check for multi/single-char operators first (>=, <=, !=, <, >, =)
	if isOperatorStart(r) {
		op, err := l.scanOperator()
		if err != nil {
			return err
		}
		l.currentToken = Token{Type: TTOperator, StringVal: op}
		return nil
	}

	switch {
	// Single-quoted string literal
	case r == '\'':
		strVal, err := l.scanString()
		if err != nil {
			return err
		}
		l.currentToken = Token{Type: TTString, StringVal: strVal}
		return nil

	// Delimiter? (commas, parentheses, semicolons, etc.)
	case isDelimiter(r):
		l.position += width
		l.currentToken = Token{Type: TTDelimiter, Rune: r}
		return nil

	// Possible date format
	case unicode.IsDigit(r):
		start := l.position
		for l.position < len(l.input) {
			r, width = utf8.DecodeRuneInString(l.input[l.position:])
			if !unicode.IsDigit(r) && r != '-' && r != ' ' && r != ':' {
				break
			}
			l.position += width
		}
		tokenStr := l.input[start:l.position]
		if t, err := parseDate(tokenStr); err == nil {
			l.currentToken = Token{Type: TTDate, TimeVal: t}
			return nil
		} else {
			// Fall back to number parsing
			// strip any whitespace
			tokenStr = strings.ReplaceAll(tokenStr, " ", "")
			if n, err := strconv.Atoi(tokenStr); err == nil {
				l.currentToken = Token{Type: TTNumber, NumVal: n}
				return nil
			} else {
				return &SyntaxError{Message: fmt.Sprintf("invalid token: '%s'", tokenStr)}
			}
		}

	// Letter/underscore => could be boolean, date, or identifier/keyword
	case unicode.IsLetter(r) || r == '_':
		wordVal := l.scanWord()
		wordValLower := strings.ToLower(wordVal)

		// Check for boolean: "true" or "false"
		if wordValLower == "true" || wordValLower == "false" {
			boolVal := wordValLower == "true"
			l.currentToken = Token{Type: TTBoolean, BoolVal: boolVal}
			return nil
		}

		// Otherwise treat as word/identifier
		l.currentToken = Token{Type: TTWord, StringVal: wordValLower}
		return nil
	}

	// If we get here, it's an unexpected character
	return &SyntaxError{
		Message: fmt.Sprintf("unexpected character '%c'", r),
	}
}

// scanOperator checks for either single- or multi-character operators
// like '=', '>', '<', '>=', '<=', '!=', '<>', etc.
func (l *Lexer) scanOperator() (string, error) {
	r, width := utf8.DecodeRuneInString(l.input[l.position:])
	l.position += width

	if l.position < len(l.input) {
		// Look ahead to see if next char is '=' or '>'
		r2, w2 := utf8.DecodeRuneInString(l.input[l.position:])

		// Combine if it forms one of the multi-char operators
		if (r == '>' && r2 == '=') || (r == '<' && r2 == '=') ||
			(r == '!' && r2 == '=') || (r == '<' && r2 == '>') {
			// e.g. ">=", "<=", "!=", "<>"
			l.position += w2
			return string([]rune{r, r2}), nil
		}
	}
	// If no multi-char operator, return single char as operator
	return string(r), nil
}

// scanString scans a single-quoted string literal.
// Returns the string value (without quotes), or an error if unterminated.
func (l *Lexer) scanString() (string, error) {
	l.position++ // consume the quote
	var sb strings.Builder

	for l.position < len(l.input) {
		r, width := utf8.DecodeRuneInString(l.input[l.position:])
		if r == '\'' {
			// Found the closing quote
			l.position += width
			return sb.String(), nil
		}
		sb.WriteRune(r)
		l.position += width
	}
	return "", &SyntaxError{Message: "unterminated string constant"}
}

// scanWord scans an identifier-like token (letters, digits, underscores).
func (l *Lexer) scanWord() string {
	start := l.position
	for l.position < len(l.input) {
		r, width := utf8.DecodeRuneInString(l.input[l.position:])
		if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_') {
			break
		}
		l.position += width
	}
	return l.input[start:l.position]
}

// skipWhitespace advances over any sequence of whitespace.
func (l *Lexer) skipWhitespace() {
	for l.position < len(l.input) {
		r, width := utf8.DecodeRuneInString(l.input[l.position:])
		if !unicode.IsSpace(r) {
			break
		}
		l.position += width
	}
}

// initKeywords initializes the set of SQL keywords, in lowercase.
func (l *Lexer) initKeywords() {
	kwList := []string{
		"select", "from", "where", "and",
		"insert", "into", "values", "delete", "update", "set",
		"create", "table", "int", "varchar", "view", "as", "index", "on",
	}
	l.keywords = make(map[string]struct{}, len(kwList))
	for _, kw := range kwList {
		l.keywords[strings.ToLower(kw)] = struct{}{}
	}
}

// parseDate attempts to parse s as a date in one of our accepted formats.
// Returns time.Time if successful, or an error if none of the formats matched.
func parseDate(s string) (time.Time, error) {
	// Accept "YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS"
	layouts := []string{
		"2006-01-02",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		t, err := time.Parse(layout, s)
		if err == nil {
			return t, nil
		}
	}
	return time.Time{}, &SyntaxError{
		Message: fmt.Sprintf("invalid date format: '%s'", s),
	}
}

// isOperatorStart returns true if this rune starts an operator (e.g. <, >, =, !).
func isOperatorStart(r rune) bool {
	switch r {
	case '<', '>', '=', '!':
		return true
	default:
		return false
	}
}

// isDelimiter checks if a rune is treated as a single-character delimiter.
// (Operators are handled separately, in isOperatorStart/scanOperator.)
func isDelimiter(r rune) bool {
	// e.g. commas, parentheses, semicolons, plus, minus, period...
	// We deliberately *exclude* <, >, =, ! so we can handle multi-char operators.
	delimiters := []rune{',', '(', ')', '.', ';', '+', '-'}
	for _, d := range delimiters {
		if r == d {
			return true
		}
	}
	return false
}
