package utils

import (
	"fmt"
	"runtime"
)

func ErrAtLine(err error) error {
	if err == nil {
		return nil
	}
	_, file, line, _ := runtime.Caller(1)
	return fmt.Errorf("%s:%d: %w", file, line, err)
}
