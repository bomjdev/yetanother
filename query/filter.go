package query

import (
	"fmt"
	"strings"
)

type Filter struct {
	Op      string
	Value   string
	Filters []Filter
}

func (f Filter) Traverse(visitor func(filter Filter) error) error {
	if err := visitor(f); err != nil {
		return err
	}
	for _, fn := range f.Filters {
		if err := fn.Traverse(visitor); err != nil {
			return err
		}
	}
	return nil
}

func (f Filter) Args() ([]string, error) {
	var args []string
	err := f.Traverse(func(fn Filter) error {
		if len(fn.Filters) == 0 {
			args = append(args, fn.Value)
		}
		return nil
	})
	return args, err
}

func (f Filter) String() string {
	if len(f.Filters) == 0 {
		if f.Op == "" {
			return f.Value
		}
		return fmt.Sprintf("%s(%s)", f.Op, f.Value)
	}
	values := make([]string, 0, len(f.Filters))
	for _, fn := range f.Filters {
		values = append(values, fn.String())
	}
	return fmt.Sprintf("%s(%s)", f.Op, strings.Join(values, ","))
}

func ParseFilter(s string) (Filter, error) {
	idx := strings.Index(s, "(")
	if idx == -1 { // simple value
		return Filter{Value: s}, nil
	}
	op := s[:idx]
	args := strings.Split(s[idx+1:len(s)-1], ",")
	f := Filter{Op: op}
	for _, arg := range args {
		sub, err := ParseFilter(arg)
		if err != nil {
			return Filter{}, fmt.Errorf("parse %q: %w", arg, err)
		}
		if sub.Op == "" {
			f.Value = sub.Value
		} else {
			f.Filters = append(f.Filters, sub)
		}
	}
	return f, nil
}
