package db

import (
	"context"
	"github.com/Masterminds/squirrel"
	"github.com/bomjdev/yetanother/query"
	"github.com/jackc/pgx/v5"
)

type RawExecFunc[T any] func(ctx context.Context, executor Executor, stmt string, args ...any) (T, error)

func (f RawExecFunc[T]) WithStatement(stmt string) StmtExecFunc[T] {
	return func(ctx context.Context, executor Executor, args ...any) (T, error) {
		return f(ctx, executor, stmt, args...)
	}
}

func (f RawExecFunc[T]) WithBuilder(builder squirrel.Sqlizer) ExecFunc[T] {
	return func(ctx context.Context, executor Executor) (T, error) {
		var zero T
		stmt, args, err := builder.ToSql()
		if err != nil {
			return zero, err
		}
		return f(ctx, executor, stmt, args...)
	}
}

func (f RawExecFunc[T]) SelectFactory(builder squirrel.SelectBuilder) func(query query.Query) (ExecFunc[T], error) {
	return func(query query.Query) (ExecFunc[T], error) {
		b, err := buildQuery(query, builder)
		if err != nil {
			return nil, err
		}
		return f.WithBuilder(b), nil
	}
}

func RawWithScanner[T any](exec RawExecFunc[pgx.Rows], scan ScanFunc[T]) RawExecFunc[T] {
	return func(ctx context.Context, executor Executor, stmt string, args ...any) (T, error) {
		var zero T
		rows, err := exec(ctx, executor, stmt, args...)
		if err != nil {
			return zero, err
		}
		return scan(rows)
	}
}
