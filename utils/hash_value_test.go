package utils

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestHashValue(t *testing.T) {
	tests := []struct {
		name    string
		input   interface{}
		wantErr bool
	}{
		{"int16", int16(123), false},
		{"int", 456, false},
		{"int64", int64(789), false},
		{"string", "test string", false},
		{"bool true", true, false},
		{"bool false", false, false},
		{"time.Time", time.Now(), false},
		{"nil", nil, true},
		{"unsupported type", struct{}{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := HashValue(tt.input)

			if tt.wantErr {
				assert.Error(t, err, "Expected an error for input: %v", tt.input)
			} else {
				require.NoError(t, err, "Did not expect an error for input: %v", tt.input)
				assert.NotEqual(t, uint32(0), got, "Hash value should not be 0 for input: %v", tt.input)
			}
		})
	}
}
