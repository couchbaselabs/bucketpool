package main

import (
	"testing"
	"time"
)

func TestResolveTimeout(t *testing.T) {
	tests := []struct {
		name      string
		flagSet   bool
		flagValue time.Duration
		raw       string
		want      time.Duration
		wantErr   bool
	}{
		{name: "neither flag nor environment", want: defaultTimeout},
		{name: "environment alone", raw: "45s", want: 45 * time.Second},
		{name: "flag alone", flagSet: true, flagValue: time.Minute, want: time.Minute},
		// The flag wins, so an unusable environment value must not fail the command.
		{name: "flag beats a bad environment value", flagSet: true, flagValue: 5 * time.Minute, raw: "5minutes", want: 5 * time.Minute},
		{name: "bad environment value alone", raw: "5minutes", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveTimeout(test.flagSet, test.flagValue, test.raw, defaultTimeout)
			if test.wantErr {
				if err == nil {
					t.Fatalf("got %s, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTimeout returned %v", err)
			}
			if got != test.want {
				t.Fatalf("got %s, want %s", got, test.want)
			}
		})
	}
}
