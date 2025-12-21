package csv_reader

import (
	"io"
)

func (r *CSVReader) CountBodyRow() int {
	if !r.initialized {
		if err := r.init(); err != nil {
			return 0
		}
	}
	return r.bodyRowCount
}

func (r *CSVReader) ReadChunk(maxChunk int) ([]CSVLine, bool, error) {
	if !r.initialized {
		if err := r.init(); err != nil {
			return nil, false, err
		}
	}

	var result []CSVLine
	count := 0

	for maxChunk <= 0 || count < maxChunk {
		// Stop if we've read all body rows
		if r.rowsReadCount >= r.bodyRowCount {
			break
		}

		record, err := r.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return result, false, err
		}

		result = append(result, CSVLine{data: record})
		count++
		r.rowsReadCount++
	}

	return result, r.rowsReadCount >= r.bodyRowCount, nil
}

func (r *CSVReader) ReadSingleRow() (CSVLine, bool, error) {
	if !r.initialized {
		if err := r.init(); err != nil {
			return CSVLine{}, false, err
		}
	}

	if r.rowsReadCount >= r.bodyRowCount {
		return CSVLine{}, true, nil
	}

	record, err := r.reader.Read()
	if err == io.EOF {
		return CSVLine{}, true, nil
	}
	if err != nil {
		return CSVLine{}, false, err
	}

	r.rowsReadCount++
	return CSVLine{data: record}, false, nil
}

func (r *CSVReader) ReadAll() ([]CSVLine, error) {
	lines, _, err := r.ReadChunk(0)
	return lines, err
}
