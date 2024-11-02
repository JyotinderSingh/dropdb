package file

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"unicode/utf8"
)

func TestPage(t *testing.T) {
	t.Run("NewPage", func(t *testing.T) {
		assert := assert.New(t)
		blockSize := 400
		page := NewPage(blockSize)
		assert.Equal(blockSize, len(page.Contents()), "Buffer size should match block size")
	})

	t.Run("NewPageFromBytes", func(t *testing.T) {
		assert := assert.New(t)
		data := []byte{1, 2, 3, 4}
		page := NewPageFromBytes(data)

		assert.Equal(len(data), len(page.Contents()), "Buffer size should match input data size")
		assert.Equal(data, page.Contents(), "Buffer contents should match input data")
	})

	t.Run("IntOperations", func(t *testing.T) {
		assert := assert.New(t)
		page := NewPage(100)
		testCases := []struct {
			offset int
			value  int32
		}{
			{0, 42},
			{4, -123},
			{8, 0},
			{12, 2147483647},  // max int32
			{16, -2147483648}, // min int32
		}

		for _, tc := range testCases {
			page.SetInt(tc.offset, tc.value)
			got := page.GetInt(tc.offset)
			assert.Equal(tc.value, got, "Integer value at offset %d should match", tc.offset)
		}
	})

	t.Run("BytesOperations", func(t *testing.T) {
		assert := assert.New(t)
		page := NewPage(100)
		testCases := []struct {
			offset int
			data   []byte
		}{
			{0, []byte{1, 2, 3, 4}},
			{20, []byte{}}, // empty array
			{40, []byte{255, 0, 255}},
			{60, make([]byte, 20)}, // zero bytes
		}

		for _, tc := range testCases {
			page.SetBytes(tc.offset, tc.data)
			got := page.GetBytes(tc.offset)
			assert.Equal(tc.data, got, "Byte data at offset %d should match", tc.offset)
		}
	})

	t.Run("StringOperations", func(t *testing.T) {
		assert := assert.New(t)
		page := NewPage(1000)
		testCases := []struct {
			offset string
			value  string
			valid  bool
		}{
			{offset: "basic", value: "Hello, World!", valid: true},
			{offset: "empty", value: "", valid: true},
			{offset: "unicode", value: "Hello, 世界!", valid: true},
			{offset: "emoji", value: "🌍🌎🌏", valid: true},
			{offset: "multiline", value: "Line 1\nLine 2", valid: true},
		}

		offset := 0
		for _, tc := range testCases {
			t.Run(tc.offset, func(t *testing.T) {
				err := page.SetString(offset, tc.value)
				if tc.valid {
					assert.NoError(err, "SetString should not fail for valid string")
					got, err := page.GetString(offset)
					assert.NoError(err, "GetString should not fail for valid string")
					assert.Equal(tc.value, got, "String value should match")
				}
				offset += MaxLength(len(tc.value)) + 8 // add some padding
			})
		}
	})

	t.Run("InvalidUTF8", func(t *testing.T) {
		assert := assert.New(t)
		page := NewPage(100)
		offset := 0

		// Create invalid UTF-8 sequence
		invalidUTF8 := []byte{0xFF, 0xFE, 0xFD}
		page.SetBytes(offset, invalidUTF8)

		_, err := page.GetString(offset)
		assert.Error(err, "GetString should fail for invalid UTF-8 sequence")
	})

	t.Run("MaxLength", func(t *testing.T) {
		assert := assert.New(t)
		testCases := []struct {
			strlen int
			want   int
		}{
			{0, 4},                       // empty string
			{1, 4 + utf8.UTFMax},         // single character
			{10, 4 + 10*utf8.UTFMax},     // 10 characters
			{1000, 4 + 1000*utf8.UTFMax}, // 1000 characters
		}

		for _, tc := range testCases {
			got := MaxLength(tc.strlen)
			assert.Equal(tc.want, got, "MaxLength for string length %d should match", tc.strlen)
		}
	})

	t.Run("BufferBoundary", func(t *testing.T) {
		assert := assert.New(t)
		blockSize := 20
		page := NewPage(blockSize)

		// Test writing at the end of buffer
		lastValidOffset := blockSize - 4 // space for one int32
		page.SetInt(lastValidOffset, 42)
		got := page.GetInt(lastValidOffset)
		assert.Equal(int32(42), got, "Value at buffer boundary should match")
	})

	t.Run("LargeData", func(t *testing.T) {
		assert := assert.New(t)
		blockSize := 1000
		page := NewPage(blockSize)

		// Create large string
		largeString := make([]byte, 500)
		for i := range largeString {
			largeString[i] = byte('A' + (i % 26))
		}

		err := page.SetString(0, string(largeString))
		assert.NoError(err, "Setting large string should not fail")

		got, err := page.GetString(0)
		assert.NoError(err, "Getting large string should not fail")
		assert.Equal(string(largeString), got, "Large string content should match")
	})
}
