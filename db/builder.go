package db

import (
	"fmt"
	"github.com/Masterminds/squirrel"
	"github.com/bomjdev/yetanother/query"
)

var Postgres = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

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
		case "contains":
			return squirrel.Like{key: fmt.Sprintf("%%%s%%", filter.Value)}, nil
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
