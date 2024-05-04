package query

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

type Query struct {
	Offset, Limit uint64
	Sort          Sort
	Filters       map[string][]Filter
}

type Sort map[string]bool

func ParseQuery(q url.Values) (Query, error) {
	query := Query{
		Sort:    make(Sort),
		Filters: make(map[string][]Filter, len(q)),
	}

	for key, values := range q {
		switch key {
		case "limit":
			limit, err := strconv.Atoi(values[0])
			if err != nil {
				return Query{}, err
			}
			query.Limit = uint64(limit)
			continue
		case "offset":
			offset, err := strconv.Atoi(values[0])
			if err != nil {
				return Query{}, err
			}
			query.Offset = uint64(offset)
			continue
		}
		for _, value := range values {
			if strings.HasPrefix(value, "sort") {
				switch value {
				case "sort(desc)":
					query.Sort[key] = true
				case "sort(asc)":
					query.Sort[key] = false
				default:
					return Query{}, fmt.Errorf("invalid sort option: %s", value)
				}
				continue
			}
			filter, err := ParseFilter(value)
			if err != nil {
				return Query{}, err
			}
			query.Filters[key] = append(query.Filters[key], filter)
		}
	}

	return query, nil
}
