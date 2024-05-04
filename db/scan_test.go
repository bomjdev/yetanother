package db

import (
	"github.com/Masterminds/squirrel"
	"kk/pkg/query"
	"testing"
)

func TestBuilder(t *testing.T) {
	psql := squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)
	sb := psql.Select("*").From("elephants")
	sb, err := buildQuery(query.Query{
		Offset: 10,
		Limit:  50,
		Sort: map[string]bool{
			"name": true,
			"id":   false,
		},
		Filters: map[string][]query.Filter{
			"name": {
				{
					Op:    "eq",
					Value: "asd",
				},
				{
					Op:    "ne",
					Value: "asd",
				},
			},
			"id": {
				{
					Op:    "eq",
					Value: "1",
				},
			},
		},
	}, sb)
	if err != nil {
		t.Fatal(err)
	}

	stmt, args, err := sb.ToSql()
	if err != nil {
		t.Fatal(err)
	}
	t.Log(stmt)
	t.Log(args)
}
