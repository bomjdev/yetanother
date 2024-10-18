package db

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var GetRows RawExecFunc[pgx.Rows] = getRows

func getRows(ctx context.Context, executor Executor, stmt string, args ...any) (pgx.Rows, error) {
	return executor.Query(ctx, stmt, args...)
}

var Exec RawExecFunc[pgconn.CommandTag] = execNoRows

func execNoRows(ctx context.Context, executor Executor, stmt string, args ...any) (pgconn.CommandTag, error) {
	return executor.Exec(ctx, stmt, args...)
}
