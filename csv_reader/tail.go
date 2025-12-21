package csv_reader

import (
	"errors"
	"fmt"
)

func (r *CSVReader) Tail(index int) (string, error) {
	if !r.initialized {
		if err := r.init(); err != nil {
			return "", err
		}
	}
	if !r.HasTail || len(r.tail) == 0 {
		return "", errors.New("no tail found")
	}
	if index < 0 || index >= len(r.tail) {
		return "", fmt.Errorf("index out of range")
	}
	return r.tail[index], nil
}

func (r *CSVReader) ValidateTail(index int, mustBe string) error {
	val, err := r.Tail(index)
	if err != nil {
		return err
	}
	if val != mustBe {
		return fmt.Errorf("tail validation failed: expected '%s', got '%s'", mustBe, val)
	}
	return nil
}

func (r *CSVReader) ValidateTailCount(expected int) error {
	if !r.initialized {
		if err := r.init(); err != nil {
			return err
		}
	}
	if !r.HasTail {
		return errors.New("no tail")
	}
	if len(r.tail) != expected {
		return fmt.Errorf("tail count mismatch: expected %d, got %d", expected, len(r.tail))
	}
	return nil
}
