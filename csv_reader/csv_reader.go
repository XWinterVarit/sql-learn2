package csv_reader

import (
	"encoding/csv"
	"io"
	"os"
)

type CSVReader struct {
	// Set
	HasHeader bool
	HasTail   bool

	// Internal
	fileName      string
	initialized   bool
	header        []string
	tail          []string
	bodyRowCount  int
	file          *os.File
	reader        *csv.Reader
	rowsReadCount int
	totalRows     int
}

func NewCSVReader(fileName string) *CSVReader {
	return &CSVReader{
		fileName: fileName,
	}
}

func (r *CSVReader) init() error {
	if r.initialized {
		return nil
	}

	f, err := os.Open(r.fileName)
	if err != nil {
		return err
	}
	// We'll keep the file open after this function returns,
	// because we'll seek back to the beginning.

	// First pass: scan to find header, tail, and count
	tempReader := csv.NewReader(f)
	// To handle variable fields per record (as headers/tails/body might differ length)
	tempReader.FieldsPerRecord = -1

	var firstRow []string
	var lastRow []string
	var count int

	for {
		record, err := tempReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			f.Close()
			return err
		}

		if count == 0 {
			firstRow = record
		}
		lastRow = record
		count++
	}

	r.totalRows = count

	var bodyCount int = count

	if r.HasHeader && count > 0 {
		r.header = firstRow
		bodyCount--
	}

	if r.HasTail && count > 0 {
		// If only 1 row and it is header, we don't have a tail.
		if r.HasHeader && count == 1 {
			// No tail
		} else {
			r.tail = lastRow
			bodyCount--
		}
	}

	if bodyCount < 0 {
		bodyCount = 0
	}
	r.bodyRowCount = bodyCount

	// Reset for reading
	_, err = f.Seek(0, 0)
	if err != nil {
		f.Close()
		return err
	}

	r.file = f
	r.reader = csv.NewReader(f)
	r.reader.FieldsPerRecord = -1

	// Skip header
	if r.HasHeader && count > 0 {
		_, err := r.reader.Read()
		if err != nil {
			// Should not happen as we just read it
			return err
		}
	}

	r.initialized = true
	return nil
}

func (r *CSVReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}
