package main

type Column struct {
	Name  *string
	value interface{}
}

type Row []Column

type Rows []Row

func (r Rows) GetColumnsNames() []string {
	if len(r) == 0 {
		return nil
	}
	var columnsNames []string
	for _, col := range r[0] {
		columnsNames = append(columnsNames, *col.Name)
	}
	return columnsNames
}

const (
	ColID       = "id"
	ColName     = "name"
	ColLastName = "lastname"
	ColBalance  = "balance"
)

func GenerateRow() Rows {
	colID := ColID
	colName := ColName
	colLastName := ColLastName
	colBalance := ColBalance
	var rows Rows
	for i := 0; i < 100; i++ {
		rows = append(rows, Row{
			Column{Name: &colID, value: 1},
			Column{Name: &colName, value: 1},
			Column{Name: &colLastName, value: 1},
			Column{Name: &colBalance, value: 1},
		})
	}
	return rows
}
