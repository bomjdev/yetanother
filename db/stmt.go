package db

import (
	"context"
	"github.com/jackc/pgx/v5"
)

type StmtExecFunc[T any] func(ctx context.Context, executor Executor, args ...any) (T, error)

func (f StmtExecFunc[T]) WithArgs(args ...any) ExecFunc[T] {
	return func(ctx context.Context, executor Executor) (T, error) {
		return f(ctx, executor, args...)
	}
}

func StmtWithScanner[T any](exec StmtExecFunc[pgx.Rows], scan ScanFunc[T]) StmtExecFunc[T] {
	return func(ctx context.Context, executor Executor, args ...any) (T, error) {
		var zero T
		rows, err := exec(ctx, executor, args...)
		if err != nil {
			return zero, err
		}
		return scan(rows)
	}
}

type StmtScanner[T any] struct {
	scan           StmtExecFunc[[]T]
	scanOne        StmtExecFunc[T]
	scanExactlyOne StmtExecFunc[T]
}

func NewStmtScanner[T any](factory StmtExecFunc[pgx.Rows]) StmtScanner[T] {
	return StmtScanner[T]{
		scan:           StmtWithScanner(factory, Scan[T]),
		scanOne:        StmtWithScanner(factory, ScanOne[T]),
		scanExactlyOne: StmtWithScanner(factory, ScanExactlyOne[T]),
	}
}

func (s StmtScanner[T]) Scan(ctx context.Context, executor Executor, args ...any) ([]T, error) {
	return s.scan(ctx, executor, args...)
}

func (s StmtScanner[T]) ScanOne(ctx context.Context, executor Executor, args ...any) (T, error) {
	return s.scanOne(ctx, executor, args...)
}

func (s StmtScanner[T]) ScanExactlyOne(ctx context.Context, executor Executor, args ...any) (T, error) {
	return s.scanExactlyOne(ctx, executor, args...)
}

func NewStmtFactory[T any](stmt string) StmtScanner[T] {
	return NewStmtScanner[T](GetRows.WithStatement(stmt))
}
