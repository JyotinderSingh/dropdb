package file

import (
	"encoding/binary"
	"errors"
	"unicode/utf8"
)

type Page struct {
	buffer []byte
}

// NewPage creates a Page with a buffer of the given block size.
func NewPage(blockSize int) *Page {
	return &Page{buffer: make([]byte, blockSize)}
}

// NewPageFromBytes creates a Page by wrapping the provided byte slice.
func NewPageFromBytes(bytes []byte) *Page {
	return &Page{buffer: bytes}
}

// GetInt retrieves a 32-bit integer from the buffer at the specified offset.
func (p *Page) GetInt(offset int) int32 {
	return int32(binary.BigEndian.Uint32(p.buffer[offset:]))
}

// SetInt writes a 32-bit integer to the buffer at the specified offset.
func (p *Page) SetInt(offset int, n int32) {
	binary.BigEndian.PutUint32(p.buffer[offset:], uint32(n))
}

// GetBytes retrieves a byte slice from the buffer starting at the specified offset.
func (p *Page) GetBytes(offset int) []byte {
	length := int(binary.BigEndian.Uint32(p.buffer[offset:]))
	start := offset + 4
	end := start + length
	b := make([]byte, length)
	copy(b, p.buffer[start:end])
	return b
}

// SetBytes writes a byte slice to the buffer starting at the specified offset.
func (p *Page) SetBytes(offset int, b []byte) {
	length := len(b)
	binary.BigEndian.PutUint32(p.buffer[offset:], uint32(length))
	start := offset + 4
	copy(p.buffer[start:], b)
}

// GetString retrieves a string from the buffer at the specified offset.
func (p *Page) GetString(offset int) (string, error) {
	b := p.GetBytes(offset)
	if !utf8.Valid(b) {
		return "", errors.New("invalid UTF-8 encoding")
	}
	return string(b), nil
}

// SetString writes a string to the buffer at the specified offset.
func (p *Page) SetString(offset int, s string) error {
	if !utf8.ValidString(s) {
		return errors.New("string contains invalid UTF-8 characters")
	}
	p.SetBytes(offset, []byte(s))
	return nil
}

// MaxLength calculates the maximum number of bytes required to store a string of a given length.
func MaxLength(strlen int) int {
	// Golang uses UTF-8 encoding
	// Add 4 bytes for the length prefix.
	return 4 + strlen*utf8.UTFMax
}

// Contents returns the byte buffer maintained by the Page.
func (p *Page) Contents() []byte {
	return p.buffer
}
