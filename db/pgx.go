package db

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"log"
)

type PGX struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) PGX {
	return PGX{
		pool: pool,
	}
}

func (p PGX) DB() *pgxpool.Pool {
	return p.pool
}

func (p PGX) Begin(ctx context.Context) (pgx.Tx, error) {
	return p.pool.Begin(ctx)
}

func (p PGX) RunInTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := p.Begin(ctx)
	defer CommitOrRollback(ctx, tx, err)
	err = fn(tx)
	return err
}

func CommitOrRollback(ctx context.Context, tx pgx.Tx, err error) {
	if err == nil {
		err = tx.Commit(ctx)
		if err != nil {
			log.Println("commit error:", err)
		}
		return
	}

	if rbErr := tx.Rollback(ctx); rbErr != nil {
		log.Println("rollback error:", rbErr, "error:", err)
	}
}
