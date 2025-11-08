package bulkinsert

import (
	"fmt"
	"testing"
)

func sampleRow(id int) Row {
	return Row{
		Column{Name: "ID", Value: id},
		Column{Name: "NAME", Value: fmt.Sprintf("Name_%d", id)},
		Column{Name: "LASTNAME", Value: fmt.Sprintf("Last_%d", id)},
		Column{Name: "BALANCE", Value: float64(id) * 10.5},
	}
}

func TestRows_GetColumnsNamesAndRows(t *testing.T) {
	rows := Rows{
		sampleRow(1),
		sampleRow(2),
	}

	names := rows.GetColumnsNames()
	wantNames := []string{"ID", "NAME", "LASTNAME", "BALANCE"}
	if len(names) != len(wantNames) {
		t.Fatalf("unexpected column names length: got %d want %d", len(names), len(wantNames))
	}
	for i := range wantNames {
		if names[i] != wantNames[i] {
			t.Fatalf("column name mismatch at %d: got %q want %q", i, names[i], wantNames[i])
		}
	}

	vals := rows.GetRows()
	if len(vals) != 2 {
		t.Fatalf("unexpected rows length: got %d want 2", len(vals))
	}
	// verify first row
	if got := vals[0][0]; got != 1 {
		t.Fatalf("row[0][0] = %v, want 1", got)
	}
	if got := vals[0][1]; got != "Name_1" {
		t.Fatalf("row[0][1] = %v, want Name_1", got)
	}
	if got := vals[0][2]; got != "Last_1" {
		t.Fatalf("row[0][2] = %v, want Last_1", got)
	}
	if got := vals[0][3]; got != 10.5 {
		t.Fatalf("row[0][3] = %v, want 10.5", got)
	}
}

func TestBulkDataBuilder_AddRowAndGetters(t *testing.T) {
	b := NewBulkDataBuilder(10)

	// Add two rows
	if err := b.AddRow(sampleRow(1)); err != nil {
		t.Fatalf("AddRow(1) error: %v", err)
	}
	if err := b.AddRow(sampleRow(2)); err != nil {
		t.Fatalf("AddRow(2) error: %v", err)
	}

	if b.GetNumRows() != 2 {
		t.Fatalf("GetNumRows() = %d, want 2", b.GetNumRows())
	}

	wantNames := []string{"ID", "NAME", "LASTNAME", "BALANCE"}
	names := b.GetColumnNames()
	if len(names) != len(wantNames) {
		t.Fatalf("unexpected column names length: got %d want %d", len(names), len(wantNames))
	}
	for i := range wantNames {
		if names[i] != wantNames[i] {
			t.Fatalf("column name mismatch at %d: got %q want %q", i, names[i], wantNames[i])
		}
	}

	colData := b.GetColumnData()
	if len(colData) != len(wantNames) {
		t.Fatalf("GetColumnData length = %d, want %d", len(colData), len(wantNames))
	}

	// Check first column (IDs)
	ids, ok := colData[0].([]interface{})
	if !ok {
		t.Fatalf("colData[0] type = %T, want []interface{}", colData[0])
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("ids = %#v, want [1 2]", ids)
	}

	// Check last column (BALANCE)
	balances, ok := colData[3].([]interface{})
	if !ok {
		t.Fatalf("colData[3] type = %T, want []interface{}", colData[3])
	}
	if len(balances) != 2 || balances[0] != 10.5 || balances[1] != 21.0 {
		t.Fatalf("balances = %#v, want [10.5 21.0]", balances)
	}
}

func TestBulkDataBuilder_AddRows(t *testing.T) {
	b := NewBulkDataBuilder(10)
	rows := Rows{sampleRow(1), sampleRow(2), sampleRow(3)}
	if err := b.AddRows(rows); err != nil {
		t.Fatalf("AddRows error: %v", err)
	}
	if b.GetNumRows() != 3 {
		t.Fatalf("GetNumRows() = %d, want 3", b.GetNumRows())
	}

	// Verify columns count
	if len(b.GetColumnData()) != 4 {
		t.Fatalf("columns = %d, want 4", len(b.GetColumnData()))
	}
}

func TestBulkDataBuilder_AddRowMismatch(t *testing.T) {
	b := NewBulkDataBuilder(10)
	if err := b.AddRow(sampleRow(1)); err != nil {
		t.Fatalf("AddRow(1) error: %v", err)
	}

	// Create a row with missing column to trigger mismatch
	badRow := Row{
		Column{Name: "ID", Value: 2},
		Column{Name: "NAME", Value: "Name_2"},
		Column{Name: "LASTNAME", Value: "Last_2"},
		// BALANCE column omitted
	}
	if err := b.AddRow(badRow); err == nil {
		t.Fatalf("expected error for mismatched columns, got nil")
	}
}

func TestBulkDataBuilder_Reset(t *testing.T) {
	b := NewBulkDataBuilder(5)
	if err := b.AddRows(Rows{sampleRow(1), sampleRow(2)}); err != nil {
		t.Fatalf("AddRows error: %v", err)
	}

	if b.GetNumRows() != 2 {
		t.Fatalf("GetNumRows before reset = %d, want 2", b.GetNumRows())
	}

	// Capture column names to ensure they persist
	namesBefore := append([]string(nil), b.GetColumnNames()...)

	b.Reset()

	if b.GetNumRows() != 0 {
		t.Fatalf("GetNumRows after reset = %d, want 0", b.GetNumRows())
	}

	namesAfter := b.GetColumnNames()
	if len(namesAfter) != len(namesBefore) {
		t.Fatalf("column names lost after reset: got %v want %v", namesAfter, namesBefore)
	}

	// Add a new row after reset and ensure data starts fresh
	if err := b.AddRow(sampleRow(3)); err != nil {
		t.Fatalf("AddRow after Reset error: %v", err)
	}
	if b.GetNumRows() != 1 {
		t.Fatalf("GetNumRows after adding = %d, want 1", b.GetNumRows())
	}

	ids, ok := b.GetColumnData()[0].([]interface{})
	if !ok || len(ids) != 1 || ids[0] != 3 {
		t.Fatalf("ids after reset = %#v, want [3]", ids)
	}
}

// Benchmarks
func BenchmarkBulkDataBuilder_AddRows(b *testing.B) {
	sizes := []int{1_000, 10_000}
	for _, n := range sizes {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				builder := NewBulkDataBuilder(n)
				for j := 0; j < n; j++ {
					if err := builder.AddRow(sampleRow(j)); err != nil {
						b.Fatalf("AddRow error: %v", err)
					}
				}
				if builder.GetNumRows() != n {
					b.Fatalf("rows = %d, want %d", builder.GetNumRows(), n)
				}
			}
		})
	}
}

// Benchmark generating and inserting wide rows (large number of columns) into the builder.
// This focuses on timing and memory behavior as column count grows.
func BenchmarkBulkDataBuilder_WideRows(b *testing.B) {
	rowCounts := []int{1_000, 5_000}
	colCounts := []int{32, 64, 128, 256}
	for _, rowsN := range rowCounts {
		for _, colsN := range colCounts {
			name := fmt.Sprintf("Rows=%d/Cols=%d", rowsN, colsN)
			b.Run(name, func(b *testing.B) {
				b.ReportAllocs()

				// Precompute column names once (avoid name allocations inside timed section)
				colNames := make([]string, colsN)
				for c := 0; c < colsN; c++ {
					colNames[c] = fmt.Sprintf("COL_%04d", c)
				}

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					builder := NewBulkDataBuilder(rowsN)

					// First row carries column names
					first := make(Row, colsN)
					for c := 0; c < colsN; c++ {
						first[c] = Column{Name: colNames[c], Value: i*1_000_000 + c}
					}
					if err := builder.AddRow(first); err != nil {
						b.Fatalf("AddRow(first) error: %v", err)
					}

					// Remaining rows use values only (names ignored by builder)
					for r := 1; r < rowsN; r++ {
						row := make(Row, colsN)
						base := i*1_000_000 + r*1_000
						for c := 0; c < colsN; c++ {
							row[c] = Column{Value: base + c}
						}
						if err := builder.AddRow(row); err != nil {
							b.Fatalf("AddRow error: %v", err)
						}
					}

					if builder.GetNumRows() != rowsN {
						b.Fatalf("rows = %d, want %d", builder.GetNumRows(), rowsN)
					}
				}
			})
		}
	}
}
