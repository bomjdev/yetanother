package db

import (
	"context"
	"github.com/bomjdev/yetanother/query"
	"github.com/jackc/pgx/v5"
)

type QueryFunc[T any] func(ctx context.Context, executor Executor, query query.Query) (T, error)

type QueryScanner[T any] struct {
	factory func(query query.Query) (ExecFunc[pgx.Rows], error)
}

func NewQueryScanner[T any](factory func(query query.Query) (ExecFunc[pgx.Rows], error)) QueryScanner[T] {
	return QueryScanner[T]{factory: factory}
}

func (s QueryScanner[T]) Scan(ctx context.Context, executor Executor, query query.Query) ([]T, error) {
	fn, err := s.factory(query)
	if err != nil {
		return nil, err
	}
	return ExecWithScanner(fn, Scan[T])(ctx, executor)
}

func (s QueryScanner[T]) ScanOne(ctx context.Context, executor Executor, query query.Query) (T, error) {
	var zero T
	fn, err := s.factory(query)
	if err != nil {
		return zero, err
	}
	return ExecWithScanner(fn, ScanOne[T])(ctx, executor)
}

func (s QueryScanner[T]) ScanExactlyOne(ctx context.Context, executor Executor, query query.Query) (T, error) {
	var zero T
	fn, err := s.factory(query)
	if err != nil {
		return zero, err
	}
	return ExecWithScanner(fn, ScanExactlyOne[T])(ctx, executor)
}
