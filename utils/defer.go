package utils

import (
	"fmt"
	"log"
)

func LogDeferredError(f func() error) {
	if err := f(); err != nil {
		log.Printf("deferred error: %s", err)
	}
}

func LogDeferredErrorFactory(l *log.Logger) func(f func() error) {
	return func(f func() error) {
		if err := f(); err != nil {
			_ = l.Output(2, fmt.Sprintf("deferred error: %s", err))
		}
	}
}
