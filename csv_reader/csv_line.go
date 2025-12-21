package csv_reader

type CSVLine struct {
	data []string
}

func (l CSVLine) CountFields() int {
	return len(l.data)
}

func (l CSVLine) Value(index int) string {
	if index < 0 || index >= len(l.data) {
		return ""
	}
	return l.data[index]
}
