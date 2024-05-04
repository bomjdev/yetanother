package db

import (
	"context"
	"fmt"
	pgxuuid "github.com/jackc/pgx-gofrs-uuid"
	pgxdecimal "github.com/jackc/pgx-shopspring-decimal"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/tracelog"
	"log"
)

type Credentials struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	SSLMode  string
}

type traceLog struct {
	l *log.Logger
}

func (t traceLog) Log(ctx context.Context, level tracelog.LogLevel, msg string, data map[string]any) {
	if msg != "Query" {
		return
	}
	_ = t.l.Output(10, fmt.Sprint(data["sql"], data["args"]))
}

func Connect(ctx context.Context, credentials Credentials) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		credentials.User,
		credentials.Password,
		credentials.Host,
		credentials.Port,
		credentials.Database,
		credentials.SSLMode,
	))
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	cfg.BeforeConnect = func(ctx context.Context, config *pgx.ConnConfig) error {
		config.Tracer = &tracelog.TraceLog{
			Logger:   traceLog{l: log.Default()},
			LogLevel: tracelog.LogLevelInfo,
		}
		return nil
	}

	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pgxdecimal.Register(conn.TypeMap())
		pgxuuid.Register(conn.TypeMap())
		return nil
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	return pool, nil
}
