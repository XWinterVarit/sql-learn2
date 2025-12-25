package rp_dynamic

import (
	"testing"
)

type TestStruct struct {
	ID   int    `db:"id"`
	Name string `db:"name"`
	Age  int    // should match by name "Age" or "age"
}

func TestStructBulkInsertBuilder(t *testing.T) {
	builder := NewStructBulkInsertBuilder[TestStruct]("users", "id", "name", "age")

	err := builder.AddRow(TestStruct{ID: 1, Name: "Alice", Age: 30})
	if err != nil {
		t.Fatalf("AddRow failed: %v", err)
	}
	err = builder.AddRow(TestStruct{ID: 2, Name: "Bob", Age: 25})
	if err != nil {
		t.Fatalf("AddRow failed: %v", err)
	}

	expectedSQL := "INSERT INTO users (id, name, age) VALUES (:1, :2, :3)"
	if sql := builder.GetSQL(); sql != expectedSQL {
		t.Errorf("GetSQL mismatch: got %s, want %s", sql, expectedSQL)
	}

	args := builder.GetArgs()
	if len(args) != 3 {
		t.Fatalf("Expected 3 columns of args, got %d", len(args))
	}

	// Verify column 1 (ID)
	ids, ok := args[0].([]interface{})
	if !ok {
		t.Fatalf("Column 0 is not []interface{}")
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Errorf("IDs mismatch: %v", ids)
	}

	// Verify column 2 (Name)
	names, ok := args[1].([]interface{})
	if !ok {
		t.Fatalf("Column 1 is not []interface{}")
	}
	if len(names) != 2 || names[0] != "Alice" || names[1] != "Bob" {
		t.Errorf("Names mismatch: %v", names)
	}

	// Verify column 3 (Age)
	ages, ok := args[2].([]interface{})
	if !ok {
		t.Fatalf("Column 2 is not []interface{}")
	}
	if len(ages) != 2 || ages[0] != 30 || ages[1] != 25 {
		t.Errorf("Ages mismatch: %v", ages)
	}
}

func TestStructBulkInsertBuilder_Ptr(t *testing.T) {
	// Testing with pointer type for T
	builder := NewStructBulkInsertBuilder[*TestStruct]("users", "id", "name")

	err := builder.AddRow(&TestStruct{ID: 1, Name: "Alice"})
	if err != nil {
		t.Fatalf("AddRow failed: %v", err)
	}

	args := builder.GetArgs()
	ids := args[0].([]interface{})
	if ids[0] != 1 {
		t.Errorf("Ptr test failed")
	}
}

func TestStructBulkInsertBuilder_MixedUsage(t *testing.T) {
	// T is struct, but passing pointer to AddRow (should work via reflection deref?)
	// My implementation handles Ptr in AddRow even if T is struct, but AddRow signature is AddRow(row T).
	// If T is struct, you can't pass *struct unless you cast or change T.
	// So T=TestStruct, AddRow takes TestStruct.
	// T=*TestStruct, AddRow takes *TestStruct.
	// So this test case is redundant if type system enforces it.
}
