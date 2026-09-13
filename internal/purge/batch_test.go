package purge

import (
	"fmt"
	"slices"
	"testing"
)

func names(count int) []string {
	made := make([]string, count)
	for i := range made {
		made[i] = fmt.Sprintf("x%d", i)
	}
	return made
}

func TestRemoveBatches(t *testing.T) {
	tests := []struct {
		name       string
		xattrs     []string
		removeBody bool
		want       [][]string
	}{
		{"nothing to remove", nil, false, nil},
		{"body alone", nil, true, [][]string{{bodyPath}}},
		{"tombstone xattrs", []string{"_sync", "_vv"}, false, [][]string{{"_sync", "_vv"}}},
		{
			"xattrs and body together",
			[]string{"_sync", "user"},
			true,
			[][]string{{"_sync", "user", bodyPath}},
		},
		{
			// 15 xattrs plus the body is exactly the limit, so one mutation still does it.
			"a full mutation",
			names(15),
			true,
			[][]string{append(names(15), bodyPath)},
		},
		{
			// The sixteenth xattr fills the first mutation, so the body moves to a second one.
			"the body overflows",
			names(16),
			true,
			[][]string{names(16), {bodyPath}},
		},
		{
			"more xattrs than one mutation holds",
			names(33),
			false,
			[][]string{names(16), names(33)[16:32], names(33)[32:]},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := removeBatches(test.xattrs, test.removeBody)
			if !slices.EqualFunc(got, test.want, slices.Equal) {
				t.Fatalf("got %q, want %q", got, test.want)
			}
			for i, batch := range got {
				if len(batch) > maxSubdocOps {
					t.Fatalf("batch %d holds %d operations, the limit is %d", i, len(batch), maxSubdocOps)
				}
			}
		})
	}
}
