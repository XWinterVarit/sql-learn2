package csvsource

import (
	"testing"
)

func TestRowParser_Int(t *testing.T) {
	p := NewRowParser()

	// Valid Int
	val := p.Int("123", "ID")
	if v, ok := val.(int); !ok || v != 123 {
		t.Errorf("expected 123, got %v", val)
	}
	if p.Err() != nil {
		t.Errorf("unexpected error: %v", p.Err())
	}

	// Invalid Int
	val = p.Int("abc", "ID")
	if val != nil {
		t.Errorf("expected nil for invalid int, got %v", val)
	}
	if p.Err() == nil {
		t.Error("expected error, got nil")
	}

	// Subsequent calls should be skipped
	val = p.Int("456", "ID2")
	if val != nil {
		t.Error("expected nil (skipped), got value")
	}
}

func TestRowParser_Float64(t *testing.T) {
	p := NewRowParser()

	// Valid Float
	val := p.Float64("123.45", "Price")
	if v, ok := val.(float64); !ok || v != 123.45 {
		t.Errorf("expected 123.45, got %v", val)
	}

	// Invalid Float
	val = p.Float64("abc", "Price")
	if val != nil {
		t.Errorf("expected nil for invalid float, got %v", val)
	}
	if p.Err() == nil {
		t.Error("expected error, got nil")
	}
}

func TestRowParser_String(t *testing.T) {
	p := NewRowParser()
	val := p.String("hello", "Name")
	if v, ok := val.(string); !ok || v != "hello" {
		t.Errorf("expected 'hello', got %v", val)
	}
}

func TestRowParser_NullableString(t *testing.T) {
	p := NewRowParser()

	// Empty string -> nil
	val := p.NullableString("", "Desc")
	if val != nil {
		t.Errorf("expected nil for empty string, got %v", val)
	}

	// Non-empty string
	val = p.NullableString("text", "Desc")
	if v, ok := val.(string); !ok || v != "text" {
		t.Errorf("expected 'text', got %v", val)
	}
}

func TestRowParser_NullableInt(t *testing.T) {
	p := NewRowParser()

	// Empty string -> nil
	val := p.NullableInt("", "Level")
	if val != nil {
		t.Errorf("expected nil for empty string, got %v", val)
	}

	// Valid Int
	val = p.NullableInt("10", "Level")
	if v, ok := val.(int); !ok || v != 10 {
		t.Errorf("expected 10, got %v", val)
	}

	// Invalid Int
	val = p.NullableInt("abc", "Level")
	if val != nil {
		t.Errorf("expected nil for invalid int, got %v", val)
	}
	if p.Err() == nil {
		t.Error("expected error, got nil")
	}
}

func TestRowParser_Chaining(t *testing.T) {
	// Test that an error stops subsequent processing
	p := NewRowParser()

	p.Int("abc", "BadInt") // Error here
	p.String("foo", "String")
	p.Float64("1.2", "Float")

	if p.Err() == nil {
		t.Fatal("expected error, got nil")
	}

	// Check that it's the first error
	// The implementation doesn't expose error details easily for structured check,
	// but we can check the string contains "BadInt"
	if err := p.Err(); err == nil || !contains(err.Error(), "BadInt") {
		t.Errorf("expected error related to BadInt, got: %v", err)
	}
}
