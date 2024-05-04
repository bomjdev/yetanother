package config

import (
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"os"
)

func ReadConfig[T any](reader io.Reader) (T, error) {
	var v T
	if err := yaml.NewDecoder(reader).Decode(&v); err != nil {
		return v, fmt.Errorf("yaml decode: %w", err)
	}
	return v, nil
}

func ReadConfigFromFile[T any](path string) (T, error) {
	var v T
	file, err := os.Open(path)
	if err != nil {
		return v, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()
	return ReadConfig[T](file)
}
