package purge

import (
	"encoding/binary"
	"testing"
)

// encodeXattrs builds a DCP value in the wire format that xattrNames parses.
func encodeXattrs(pairs [][2]string, body string) []byte {
	section := []byte{}
	for _, pair := range pairs {
		encoded := append([]byte(pair[0]), 0)
		encoded = append(encoded, []byte(pair[1])...)
		encoded = append(encoded, 0)
		section = binary.BigEndian.AppendUint32(section, uint32(len(encoded)))
		section = append(section, encoded...)
	}
	value := binary.BigEndian.AppendUint32(nil, uint32(len(section)))
	value = append(value, section...)
	return append(value, []byte(body)...)
}

func TestXattrNames(t *testing.T) {
	tests := []struct {
		name  string
		value []byte
		want  []string
	}{
		{"no value", nil, nil},
		{"empty section", encodeXattrs(nil, `{"a":1}`), nil},
		{"one xattr", encodeXattrs([][2]string{{"_sync", "{}"}}, `{"a":1}`), []string{"_sync"}},
		{
			"several xattrs",
			encodeXattrs([][2]string{{"_sync", `{"rev":"1-a"}`}, {"_vv", "{}"}, {"user", "1"}}, `{"a":1}`),
			[]string{"_sync", "_vv", "user"},
		},
		// A tombstone carries its xattrs with no body behind them.
		{"tombstone", encodeXattrs([][2]string{{"_sync", "{}"}}, ""), []string{"_sync"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := xattrNames(test.value)
			if err != nil {
				t.Fatalf("xattrNames returned %v", err)
			}
			if len(got) != len(test.want) {
				t.Fatalf("got %v, want %v", got, test.want)
			}
			for i := range got {
				if got[i] != test.want[i] {
					t.Fatalf("got %v, want %v", got, test.want)
				}
			}
		})
	}
}

func TestXattrNamesRejectsBadLengths(t *testing.T) {
	t.Run("section longer than value", func(t *testing.T) {
		value := binary.BigEndian.AppendUint32(nil, 100)
		if _, err := xattrNames(append(value, 1, 2, 3, 4)); err == nil {
			t.Fatal("expected an error for a section length past the end of the value")
		}
	})

	t.Run("zero pair length", func(t *testing.T) {
		value := binary.BigEndian.AppendUint32(nil, 4)
		value = binary.BigEndian.AppendUint32(value, 0)
		if _, err := xattrNames(value); err == nil {
			t.Fatal("expected an error for a zero-length xattr pair")
		}
	})
}

// The lengths come off the wire as uint32, so a hostile one must not wrap past a bounds check
// and panic in the slice that follows it.
func TestXattrNamesRejectsOverflowingLengths(t *testing.T) {
	t.Run("pair length wraps", func(t *testing.T) {
		value := binary.BigEndian.AppendUint32(nil, 8)
		value = binary.BigEndian.AppendUint32(value, 0xFFFFFFFF)
		value = append(value, 'a', 0)
		if _, err := xattrNames(value); err == nil {
			t.Fatal("expected an error for a pair length past the end of the section")
		}
	})

	t.Run("section length wraps", func(t *testing.T) {
		value := binary.BigEndian.AppendUint32(nil, 0xFFFFFFFF)
		if _, err := xattrNames(append(value, 1, 2, 3, 4)); err == nil {
			t.Fatal("expected an error for a section length past the end of the value")
		}
	})
}

func TestXattrNamesRejectsAMissingTerminator(t *testing.T) {
	value := binary.BigEndian.AppendUint32(nil, 7)
	value = binary.BigEndian.AppendUint32(value, 3)
	value = append(value, 'a', 'b', 'c')
	if _, err := xattrNames(value); err == nil {
		t.Fatal("expected an error for an xattr pair with no name terminator")
	}
}
