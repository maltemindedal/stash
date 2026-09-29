package storage

import (
	"errors"
	"testing"
	"time"
)

func TestParseExpiryMillis(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantAdd int64 // expected deadline, in milliseconds after now
		wantErr error
	}{
		{name: "no expiry", args: nil},
		{name: "one second", args: []string{"EX", "1"}, wantAdd: 1000},
		{name: "lowercase unit", args: []string{"ex", "5"}, wantAdd: 5000},
		{name: "milliseconds", args: []string{"PX", "1500"}, wantAdd: 1500},
		// These wrapped negative in time.Duration arithmetic: the first made the key
		// permanent (a negative deadline reads as "no expiry"), the others expired it
		// at once. They are far-future deadlines and must be accepted as such.
		{name: "seconds beyond time.Duration", args: []string{"EX", "9223372037"}, wantAdd: 9223372037 * 1000},
		{name: "milliseconds beyond time.Duration", args: []string{"PX", "9223372036855"}, wantAdd: 9223372036855},
		{name: "seconds far beyond time.Duration", args: []string{"EX", "10000000000"}, wantAdd: 10000000000 * 1000},
		// A deadline that does not fit in int64 milliseconds is rejected, not wrapped.
		{name: "seconds that overflow int64 milliseconds", args: []string{"EX", "9223372036854775807"}, wantErr: ErrInvalidExpireTime},
		{name: "seconds just over the limit", args: []string{"EX", "9223372036854776"}, wantErr: ErrInvalidExpireTime},
		{name: "milliseconds that overflow", args: []string{"PX", "9223372036854775807"}, wantErr: ErrInvalidExpireTime},
		{name: "zero", args: []string{"EX", "0"}, wantErr: ErrInvalidExpireTime},
		{name: "negative", args: []string{"PX", "-5"}, wantErr: ErrInvalidExpireTime},
		{name: "not a number", args: []string{"EX", "soon"}, wantErr: ErrInvalidExpireTime},
		{name: "unknown unit", args: []string{"MX", "5"}, wantErr: ErrSyntax},
		{name: "missing value", args: []string{"EX"}, wantErr: ErrSyntax},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := make([][]byte, len(tt.args))
			for i, arg := range tt.args {
				args[i] = []byte(arg)
			}

			before := time.Now()
			got, err := ParseExpiryMillis(args)
			after := time.Now()

			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ParseExpiryMillis(%q) error = %v, want %v", tt.args, err, tt.wantErr)
			}
			if tt.wantErr != nil {
				return
			}
			if tt.wantAdd == 0 {
				if got != 0 {
					t.Fatalf("ParseExpiryMillis(%q) = %d, want 0 for no expiry", tt.args, got)
				}
				return
			}
			low, high := before.UnixMilli()+tt.wantAdd, after.UnixMilli()+tt.wantAdd
			if got < low || got > high {
				t.Fatalf("ParseExpiryMillis(%q) = %d, want within [%d, %d]", tt.args, got, low, high)
			}
		})
	}
}
