package parse

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestLexer_MatchDelim(t *testing.T) {
	lexer := NewLexer(",")
	assert.True(t, lexer.MatchDelim(','), "Expected true for matching delimiter")
	assert.False(t, lexer.MatchDelim(';'), "Expected false for non-matching delimiter")
}

func TestLexer_MatchIntConstant(t *testing.T) {
	lexer := NewLexer("42")
	assert.True(t, lexer.MatchIntConstant(), "Expected true for matching integer constant")
}

func TestLexer_MatchStringConstant(t *testing.T) {
	lexer := NewLexer("'hello'")
	assert.True(t, lexer.MatchStringConstant(), "Expected true for matching string constant")
}

func TestLexer_MatchKeyword(t *testing.T) {
	lexer := NewLexer("SELECT")
	assert.True(t, lexer.MatchKeyword("select"), "Expected true for matching keyword")
	assert.False(t, lexer.MatchKeyword("insert"), "Expected false for non-matching keyword")
}

func TestLexer_MatchBooleanConstant(t *testing.T) {
	lexer := NewLexer("true")
	assert.True(t, lexer.MatchBooleanConstant(), "Expected true for matching boolean constant")

	lexer = NewLexer("false")
	assert.True(t, lexer.MatchBooleanConstant(), "Expected true for matching boolean constant")
}

func TestLexer_MatchDateConstant(t *testing.T) {
	lexer := NewLexer("2025-01-06")
	assert.True(t, lexer.MatchDateConstant(), "Expected true for matching date constant")
}

func TestLexer_MatchOperator(t *testing.T) {
	lexer := NewLexer("=")
	assert.True(t, lexer.MatchOperator("="), "Expected true for matching operator")
	assert.False(t, lexer.MatchOperator(">="), "Expected false for non-matching operator")
}

func TestLexer_EatIntConstant(t *testing.T) {
	lexer := NewLexer("42")
	val, err := lexer.EatIntConstant()
	assert.NoError(t, err, "Unexpected error for EatIntConstant")
	assert.Equal(t, 42, val, "Expected value to be 42")
}

func TestLexer_EatStringConstant(t *testing.T) {
	lexer := NewLexer("'HELLO'")
	val, err := lexer.EatStringConstant()
	assert.NoError(t, err, "Unexpected error for EatStringConstant")
	assert.Equal(t, "HELLO", val, "Expected value to be 'hello'")
}

func TestLexer_EatIdentifier(t *testing.T) {
	lexer := NewLexer("TABLE_NAME")
	val, err := lexer.EatId()
	assert.NoError(t, err, "Unexpected error for EatStringConstant: %v", err)
	assert.Equal(t, "table_name", val, "Expected value to be 'hello'")

	lexer = NewLexer("table_name")
	val, err = lexer.EatId()
	assert.NoError(t, err, "Unexpected error for EatStringConstant: %v", err)
	assert.Equal(t, "table_name", val, "Expected value to be 'hello'")
}

func TestLexer_EatKeyword(t *testing.T) {
	lexer := NewLexer("SELECT")
	assert.NoError(t, lexer.EatKeyword("select"), "Unexpected error for EatKeyword")
}

func TestLexer_EatBooleanConstant(t *testing.T) {
	lexer := NewLexer("true")
	val, err := lexer.EatBooleanConstant()
	assert.NoError(t, err, "Unexpected error for EatBooleanConstant")
	assert.True(t, val, "Expected value to be true")
}

func TestLexer_EatDateConstant(t *testing.T) {
	lexer := NewLexer("2025-01-06")
	expectedDate, _ := time.Parse("2006-01-02", "2025-01-06")
	val, err := lexer.EatDateConstant()
	assert.NoError(t, err, "Unexpected error for EatDateConstant")
	assert.True(t, val.Equal(expectedDate), "Expected date to match")
}

func TestLexer_EatOperator(t *testing.T) {
	lexer := NewLexer("=")
	assert.NoError(t, lexer.EatOperator("="), "Unexpected error for EatOperator")

	lexer = NewLexer(">=")
	assert.NoError(t, lexer.EatOperator(">="), "Unexpected error for EatOperator")

	lexer = NewLexer(">")
	assert.NoError(t, lexer.EatOperator(">"), "Unexpected error for EatOperator")

	lexer = NewLexer("!=")
	assert.NoError(t, lexer.EatOperator("!="), "Unexpected error for EatOperator")
}

func TestLexer_EatDelim(t *testing.T) {
	lexer := NewLexer(",")
	assert.NoError(t, lexer.EatDelim(','), "Unexpected error for EatDelim")

	lexer = NewLexer(";")
	assert.NoError(t, lexer.EatDelim(';'), "Unexpected error for EatDelim")

	lexer = NewLexer(")")
	assert.NoError(t, lexer.EatDelim(')'), "Unexpected error for EatDelim")

	lexer = NewLexer("(")
	assert.NoError(t, lexer.EatDelim('('), "Unexpected error for EatDelim")

	lexer = NewLexer(".")
	assert.NoError(t, lexer.EatDelim('.'), "Unexpected error for EatDelim")
}

func TestLexer_EatUnexpectedOperator(t *testing.T) {
	lexer := NewLexer(">")
	err := lexer.EatOperator("=")
	assert.Error(t, err, "Expected error for EatOperator")
	var syntaxErr *SyntaxError
	assert.ErrorAs(t, err, &syntaxErr, "Expected SyntaxError type")
	assert.Equal(t, "expected operator '='", syntaxErr.Message, "Unexpected error message")
}

func TestLexer_SyntaxError(t *testing.T) {
	lexer := NewLexer("abc")
	_, err := lexer.EatIntConstant()
	assert.Error(t, err, "Expected error for EatIntConstant")
	var syntaxErr *SyntaxError
	assert.ErrorAs(t, err, &syntaxErr, "Expected SyntaxError type")
	assert.Equal(t, "expected integer constant", syntaxErr.Message, "Unexpected error message")
}
