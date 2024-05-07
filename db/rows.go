package db

import (
	"context"
	"github.com/jackc/pgx/v5"
)

var GetRows RawExecFunc[pgx.Rows] = getRows

func getRows(ctx context.Context, executor Executor, stmt string, args ...any) (pgx.Rows, error) {
	return executor.Query(ctx, stmt, args...)
}
