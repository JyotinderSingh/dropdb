package file

import (
	"encoding/binary"
	"errors"
	"github.com/JyotinderSingh/dropdb/utils"
	"runtime"
	"time"
	"unicode/utf8"
)

// Page represents a page in the database file.
// A page is a fixed-size block of data that is read from or written to disk as a unit.
// The size of a page is determined by the file manager and is typically a multiple of the disk block size.
// Pages are the unit of transfer between disk and main memory.
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

// GetInt retrieves an integer from the buffer at the specified offset.
func (p *Page) GetInt(offset int) int {
	if runtime.GOARCH == "386" || runtime.GOARCH == "arm" {
		return int(binary.BigEndian.Uint32(p.buffer[offset:]))
	}
	// arm64 (M1/M2 Macs) and amd64 use 64-bit
	return int(binary.BigEndian.Uint64(p.buffer[offset:]))
}

// SetInt writes an integer to the buffer at the specified offset.
func (p *Page) SetInt(offset int, n int) {
	if runtime.GOARCH == "386" || runtime.GOARCH == "arm" {
		binary.BigEndian.PutUint32(p.buffer[offset:], uint32(n))
	} else {
		binary.BigEndian.PutUint64(p.buffer[offset:], uint64(n))
	}
}

// GetLong retrieves a 64-bit integer from the buffer at the specified offset.
func (p *Page) GetLong(offset int) int64 {
	return int64(binary.BigEndian.Uint64(p.buffer[offset:]))
}

// SetLong writes a 64-bit integer to the buffer at the specified offset.
func (p *Page) SetLong(offset int, n int64) {
	binary.BigEndian.PutUint64(p.buffer[offset:], uint64(n))
}

// GetBytes retrieves a byte slice from the buffer starting at the specified offset.
func (p *Page) GetBytes(offset int) []byte {
	length := binary.BigEndian.Uint32(p.buffer[offset:])
	start := offset + utils.IntSize
	end := start + int(length)
	b := make([]byte, length)
	copy(b, p.buffer[start:end])
	return b
}

// SetBytes writes a byte slice to the buffer starting at the specified offset.
func (p *Page) SetBytes(offset int, b []byte) {
	length := len(b)
	binary.BigEndian.PutUint32(p.buffer[offset:], uint32(length))
	start := offset + utils.IntSize
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
	// Add utils.IntSize bytes for the length prefix.
	return utils.IntSize + strlen*utf8.UTFMax
}

// Contents returns the byte buffer maintained by the Page.
func (p *Page) Contents() []byte {
	return p.buffer
}
