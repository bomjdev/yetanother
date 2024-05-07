package db

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type ExecFunc[T any] func(ctx context.Context, executor Executor) (T, error)

func ExecWithScanner[T any](exec ExecFunc[pgx.Rows], scan ScanFunc[T]) ExecFunc[T] {
	return func(ctx context.Context, executor Executor) (T, error) {
		var zero T
		rows, err := exec(ctx, executor)
		if err != nil {
			return zero, err
		}
		return scan(rows)
	}
}

type ExecScanner[T any] struct {
	scan           ExecFunc[[]T]
	scanOne        ExecFunc[T]
	scanExactlyOne ExecFunc[T]
}

func NewExecScanner[T any](factory ExecFunc[pgx.Rows]) ExecScanner[T] {
	return ExecScanner[T]{
		scan:           ExecWithScanner(factory, Scan[T]),
		scanOne:        ExecWithScanner(factory, ScanOne[T]),
		scanExactlyOne: ExecWithScanner(factory, ScanExactlyOne[T]),
	}
}

func (s ExecScanner[T]) Scan(ctx context.Context, executor Executor) ([]T, error) {
	return s.scan(ctx, executor)
}

func (s ExecScanner[T]) ScanOne(ctx context.Context, executor Executor) (T, error) {
	return s.scanOne(ctx, executor)
}

func (s ExecScanner[T]) ScanExactlyOne(ctx context.Context, executor Executor) (T, error) {
	return s.scanExactlyOne(ctx, executor)
}
