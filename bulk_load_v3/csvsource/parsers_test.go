package csvsource

import (
	"testing"
)

func TestParseInt(t *testing.T) {
	tests := []struct {
		input    string
		expected int
		wantErr  bool
	}{
		{"123", 123, false},
		{"-5", -5, false},
		{"0", 0, false},
		{"abc", 0, true},
		{"12.34", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		val, err := ParseInt(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseInt(%q) expected error, got nil", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("ParseInt(%q) unexpected error: %v", tt.input, err)
			}
			if val.(int) != tt.expected {
				t.Errorf("ParseInt(%q) = %v, want %v", tt.input, val, tt.expected)
			}
		}
	}
}

func TestParseFloat(t *testing.T) {
	tests := []struct {
		input    string
		expected float64
		wantErr  bool
	}{
		{"123.45", 123.45, false},
		{"-5.5", -5.5, false},
		{"0", 0, false},
		{"abc", 0, true},
		{"", 0, true},
	}

	for _, tt := range tests {
		val, err := ParseFloat(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseFloat(%q) expected error, got nil", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("ParseFloat(%q) unexpected error: %v", tt.input, err)
			}
			if val.(float64) != tt.expected {
				t.Errorf("ParseFloat(%q) = %v, want %v", tt.input, val, tt.expected)
			}
		}
	}
}

func TestParseString(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"hello", "hello"},
		{"", ""},
		{"123", "123"},
	}

	for _, tt := range tests {
		val, err := ParseString(tt.input)
		if err != nil {
			t.Errorf("ParseString(%q) unexpected error: %v", tt.input, err)
		}
		if val.(string) != tt.expected {
			t.Errorf("ParseString(%q) = %v, want %v", tt.input, val, tt.expected)
		}
	}
}

func TestParseNullableString(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
	}{
		{"hello", "hello"},
		{"", nil},
	}

	for _, tt := range tests {
		val, err := ParseNullableString(tt.input)
		if err != nil {
			t.Errorf("ParseNullableString(%q) unexpected error: %v", tt.input, err)
		}
		if val != tt.expected {
			t.Errorf("ParseNullableString(%q) = %v, want %v", tt.input, val, tt.expected)
		}
	}
}

func TestParseNullableInt(t *testing.T) {
	tests := []struct {
		input    string
		expected interface{}
		wantErr  bool
	}{
		{"123", 123, false},
		{"", nil, false},
		{"abc", nil, true},
	}

	for _, tt := range tests {
		val, err := ParseNullableInt(tt.input)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParseNullableInt(%q) expected error, got nil", tt.input)
			}
		} else {
			if err != nil {
				t.Errorf("ParseNullableInt(%q) unexpected error: %v", tt.input, err)
			}
			if val != tt.expected {
				t.Errorf("ParseNullableInt(%q) = %v, want %v", tt.input, val, tt.expected)
			}
		}
	}
}
