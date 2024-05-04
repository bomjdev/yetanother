package utils

import (
	"fmt"
	"log"
	"os"
)

func Must[T any](v T, err error) T {
	if err != nil {
		if logErr := log.Output(2, err.Error()); logErr != nil {
			panic(fmt.Errorf("logger output: %w, original error: %w", logErr, err))
		}
		os.Exit(1)
	}
	return v
}
