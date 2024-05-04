package db

import (
	"context"
	"errors"
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/bomjdev/yetanother/query"
	"github.com/jackc/pgx/v5"
)

func ScanOne[T any](rows pgx.Rows) (T, error) {
	v, err := pgx.CollectOneRow(rows, pgx.RowToStructByName[T])
	if err != nil {
		return v, fmt.Errorf("collect one row: %w", err)
	}
	return v, nil
}

func Scan[T any](rows pgx.Rows) ([]T, error) {
	v, err := pgx.CollectRows(rows, pgx.RowToStructByName[T])
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("%w: %w", ErrNotFound, err)
	}
	if err != nil {
		return nil, fmt.Errorf("collect rows: %w", err)
	}
	return v, nil
}

func Query[T any](query string) func(ctx context.Context, executor Executor, args ...any) ([]T, error) {
	return func(ctx context.Context, executor Executor, args ...any) ([]T, error) {
		rows, err := executor.Query(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return Scan[T](rows)
	}
}

func QueryOne[T any](query string) func(ctx context.Context, executor Executor, args ...any) (T, error) {
	return func(ctx context.Context, executor Executor, args ...any) (T, error) {
		rows, err := executor.Query(ctx, query, args...)
		if err != nil {
			return *new(T), err
		}
		return ScanOne[T](rows)
	}
}

func Query2[T any](builder squirrel.SelectBuilder) func(ctx context.Context, executor Executor, query query.Query) ([]T, error) {
	return func(ctx context.Context, executor Executor, query query.Query) ([]T, error) {
		stmt, args, err := BuildQuery(query, builder)
		if err != nil {
			return nil, err
		}
		rows, err := executor.Query(ctx, stmt, args...)
		if err != nil {
			return nil, err
		}
		return Scan[T](rows)
	}
}

func QueryOne2[T any](builder squirrel.SelectBuilder) func(ctx context.Context, executor Executor, query query.Query) (T, error) {
	return func(ctx context.Context, executor Executor, query query.Query) (T, error) {
		var v T
		stmt, args, err := BuildQuery(query, builder)
		if err != nil {
			return v, err
		}
		rows, err := executor.Query(ctx, stmt, args...)
		if err != nil {
			return v, err
		}
		return ScanOne[T](rows)
	}
}

func QueryOneOrError[T any](builder squirrel.SelectBuilder) func(ctx context.Context, executor Executor, query query.Query) (T, error) {
	f := Query2[T](builder)
	return func(ctx context.Context, executor Executor, query query.Query) (T, error) {
		var v T
		rows, err := f(ctx, executor, query)
		if err != nil {
			return v, err
		}
		if len(rows) != 1 {
			return v, fmt.Errorf("expected 1 row, got %d", len(rows))
		}
		return rows[0], nil
	}
}

func GetSelectBuilder(builder squirrel.StatementBuilderType, table string, columns ...string) squirrel.SelectBuilder {
	return builder.Select(columns...).From(table)
}

func BuildQuery(query query.Query, builder squirrel.SelectBuilder) (string, []any, error) {
	b, err := buildQuery(query, builder)
	if err != nil {
		return "", nil, err
	}
	return b.ToSql()
}

func buildQuery(query query.Query, builder squirrel.SelectBuilder) (squirrel.SelectBuilder, error) {
	if query.Offset > 0 {
		builder = builder.Offset(query.Offset)
	}
	if query.Limit > 0 {
		builder = builder.Limit(query.Limit)
	}
	sorts := make([]string, 0, len(query.Sort))
	for col, desc := range query.Sort {
		if desc {
			sorts = append(sorts, fmt.Sprintf("%s DESC", col))
		} else {
			sorts = append(sorts, fmt.Sprintf("%s ASC", col))
		}
	}
	builder = builder.OrderBy(sorts...)
	clauses := make([]squirrel.Sqlizer, 0, len(query.Filters))
	for col, filters := range query.Filters {
		for _, filter := range filters {
			s, err := recursiveBuildWhere(col, filter)
			if err != nil {
				return squirrel.SelectBuilder{}, err
			}
			clauses = append(clauses, s)
		}
	}
	builder = builder.Where(squirrel.And(clauses))
	return builder, nil
}

func recursiveBuildWhere(key string, filter query.Filter) (squirrel.Sqlizer, error) {
	if len(filter.Filters) == 0 {
		switch filter.Op {
		case "eq", "in", "":
			return squirrel.Eq{key: filter.Value}, nil
		case "ne":
			return squirrel.NotEq{key: filter.Value}, nil
		case "gt":
			return squirrel.Gt{key: filter.Value}, nil
		case "ge":
			return squirrel.GtOrEq{key: filter.Value}, nil
		case "lt":
			return squirrel.Lt{key: filter.Value}, nil
		case "le":
			return squirrel.LtOrEq{key: filter.Value}, nil
		default:
			return nil, fmt.Errorf("unknown filter op %q", filter.Op)
		}
	}
	clauses := make([]squirrel.Sqlizer, 0, len(filter.Filters))
	for _, f := range filter.Filters {
		clause, err := recursiveBuildWhere(key, f)
		if err != nil {
			return nil, err
		}
		clauses = append(clauses, clause)
	}
	switch filter.Op {
	case "and":
		return squirrel.And(clauses), nil
	case "or":
		return squirrel.Or(clauses), nil
	default:
		return nil, fmt.Errorf("operator %q is not supported", filter.Op)
	}
}
