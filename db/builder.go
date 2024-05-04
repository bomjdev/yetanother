package db

import "github.com/Masterminds/squirrel"

var Postgres = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
