package csv_reader

import (
	"errors"
	"fmt"
)

func (r *CSVReader) Header(index int) (string, error) {
	if !r.initialized {
		if err := r.init(); err != nil {
			return "", err
		}
	}
	if !r.HasHeader || len(r.header) == 0 {
		return "", errors.New("no header found")
	}
	if index < 0 || index >= len(r.header) {
		return "", fmt.Errorf("index out of range")
	}
	return r.header[index], nil
}

func (r *CSVReader) ValidateHeader(index int, mustBe string) error {
	val, err := r.Header(index)
	if err != nil {
		return err
	}
	if val != mustBe {
		return fmt.Errorf("header validation failed: expected '%s', got '%s'", mustBe, val)
	}
	return nil
}

func (r *CSVReader) ValidateHeaderCount(expected int) error {
	if !r.initialized {
		if err := r.init(); err != nil {
			return err
		}
	}
	if !r.HasHeader {
		return errors.New("no header")
	}
	if len(r.header) != expected {
		return fmt.Errorf("header count mismatch: expected %d, got %d", expected, len(r.header))
	}
	return nil
}
