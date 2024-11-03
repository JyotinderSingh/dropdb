package file

import (
	"encoding/binary"
	"errors"
	"time"
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

// GetShort retrieves a 16-bit integer from the buffer at the specified offset.
func (p *Page) GetShort(offset int) int16 {
	return int16(binary.BigEndian.Uint16(p.buffer[offset:]))
}

// SetShort writes a 16-bit integer to the buffer at the specified offset.
func (p *Page) SetShort(offset int, n int16) {
	binary.BigEndian.PutUint16(p.buffer[offset:], uint16(n))
}

// GetBool retrieves a boolean from the buffer at the specified offset.
func (p *Page) GetBool(offset int) bool {
	return p.buffer[offset] != 0
}

// SetBool writes a boolean to the buffer at the specified offset.
func (p *Page) SetBool(offset int, b bool) {
	if b {
		p.buffer[offset] = 1
	} else {
		p.buffer[offset] = 0
	}
}

// GetDate retrieves a date (stored as a Unix timestamp) from the buffer at the specified offset.
func (p *Page) GetDate(offset int) time.Time {
	unixTimestamp := int64(binary.BigEndian.Uint64(p.buffer[offset:]))
	return time.Unix(unixTimestamp, 0)
}

// SetDate writes a date (as a Unix timestamp) to the buffer at the specified offset.
func (p *Page) SetDate(offset int, date time.Time) {
	binary.BigEndian.PutUint64(p.buffer[offset:], uint64(date.Unix()))
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
