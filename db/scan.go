package db

import (
	"fmt"
	"github.com/jackc/pgx/v5"
)

type ScanFunc[T any] func(rows pgx.Rows) (T, error)

func Scan[T any](rows pgx.Rows) ([]T, error) {
	v, err := pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if err != nil {
		return nil, fmt.Errorf("collect rows: %w", err)
	}
	return v, nil
}

func ScanOne[T any](rows pgx.Rows) (T, error) {
	v, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
	if err != nil {
		return v, fmt.Errorf("collect one row: %w", err)
	}
	return v, nil
}

func ScanExactlyOne[T any](rows pgx.Rows) (T, error) {
	v, err := pgx.CollectExactlyOneRow(rows, pgx.RowToStructByName[T])
	if err != nil {
		return v, fmt.Errorf("collect one row: %w", err)
	}
	return v, nil
}
