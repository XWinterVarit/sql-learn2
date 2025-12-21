package csv_reader

import (
	"os"
	"testing"
)

func TestCSVReader(t *testing.T) {
	// Create temp file
	content := "h1,h2\nv1,v2\nv3,v4\nt1,t2"
	f, err := os.CreateTemp("", "test*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString(content)
	f.Close()

	t.Run("FullConfig", func(t *testing.T) {
		r := NewCSVReader(f.Name())
		r.HasHeader = true
		r.HasTail = true

		if count := r.CountBodyRow(); count != 2 {
			t.Errorf("Expected 2 body rows, got %d", count)
		}

		h, err := r.Header(0)
		if err != nil || h != "h1" {
			t.Errorf("Header mismatch: %v, %v", h, err)
		}

		tail, err := r.Tail(0)
		if err != nil || tail != "t1" {
			t.Errorf("Tail mismatch: %v, %v", tail, err)
		}

		lines, isEnded, err := r.ReadChunk(1)
		if err != nil {
			t.Fatal(err)
		}
		if isEnded {
			t.Errorf("Expected not ended, got ended")
		}
		if len(lines) != 1 {
			t.Fatalf("Expected lines len 1, got %d", len(lines))
		}
		if lines[0].Value(0) != "v1" {
			t.Errorf("Value mismatch: %s", lines[0].Value(0))
		}

		lines, isEnded, err = r.ReadChunk(10)
		if !isEnded {
			t.Errorf("Expected ended, got not ended")
		}
		if len(lines) != 1 {
			t.Errorf("Expected 1 remaining line, got %d", len(lines))
		}
		if lines[0].Value(0) != "v3" {
			t.Errorf("Value mismatch: %s", lines[0].Value(0))
		}

		// Next read should be empty
		lines, isEnded, err = r.ReadChunk(1)
		if !isEnded {
			t.Errorf("Expected ended, got not ended")
		}
		if len(lines) != 0 {
			t.Errorf("Expected 0 lines, got %d", len(lines))
		}
	})

	t.Run("NoConfig", func(t *testing.T) {
		r := NewCSVReader(f.Name())
		r.HasHeader = false
		r.HasTail = false

		if count := r.CountBodyRow(); count != 4 {
			t.Errorf("Expected 4 body rows, got %d", count)
		}
	})
}

func TestReadSingleRow(t *testing.T) {
	// Create temp file
	content := "v1,v2\nv3,v4"
	f, err := os.CreateTemp("", "test_single*.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	f.WriteString(content)
	f.Close()

	r := NewCSVReader(f.Name())
	r.HasHeader = false
	r.HasTail = false

	// Read 1st row
	line, done, err := r.ReadSingleRow()
	if err != nil {
		t.Fatalf("ReadSingleRow 1 failed: %v", err)
	}
	if done {
		t.Fatal("Expected not done")
	}
	if line.Value(0) != "v1" {
		t.Errorf("Expected v1, got %s", line.Value(0))
	}

	// Read 2nd row
	line, done, err = r.ReadSingleRow()
	if err != nil {
		t.Fatalf("ReadSingleRow 2 failed: %v", err)
	}
	if done {
		t.Fatal("Expected not done")
	}
	if line.Value(0) != "v3" {
		t.Errorf("Expected v3, got %s", line.Value(0))
	}

	// Read 3rd row (should be done)
	line, done, err = r.ReadSingleRow()
	if err != nil {
		t.Errorf("Expected nil error, got %v", err)
	}
	if !done {
		t.Errorf("Expected done=true")
	}
	// Verify empty line
	if line.CountFields() != 0 {
		t.Errorf("Expected empty line, got fields: %d", line.CountFields())
	}
}
