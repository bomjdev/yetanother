package query

import (
	"net/url"
	"testing"
)

func TestQuery(t *testing.T) {
	type testCase struct {
		fn string
	}
	for _, tc := range []testCase{
		{fn: "and(eq(1),not(eq(0)))"},
		{fn: "and()"},
		{fn: "and"},
		{fn: "contains(1,2,3)"},
		{fn: "contains(q,w,e)"},
	} {
		t.Log(tc.fn)
		q, err := ParseFilter(tc.fn)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(q)
	}
}

func TestRequestQuery(t *testing.T) {
	type testCase struct {
		query url.Values
	}
	for _, tc := range []testCase{
		{query: url.Values{
			"id": []string{
				"not(eq(1))",
				"and(not(eq(1)),gt(5),le(10))",
				"sort(desc)",
			},
		}},
	} {
		t.Log(tc.query)
		t.Log(tc.query.Encode())

		q, err := ParseQuery(tc.query)
		if err != nil {
			t.Fatal(err)
		}
		t.Log(q)
	}
}
