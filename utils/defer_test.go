package utils

import (
	"errors"
	"log"
	"testing"
)

func TestLogDeferredError(t *testing.T) {
	log.SetFlags(log.Llongfile)
	lde := LogDeferredErrorFactory(log.Default())
	defer lde(returnError)
}

func returnError() error {
	return errors.New("test error")
}
