package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/couchbaselabs/bucketpool/internal/purge"
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
			got, err := resolve(envTimeout, test.flagSet, test.flagValue, test.raw, defaultTimeout, time.ParseDuration)
			if test.wantErr {
				if err == nil {
					t.Fatalf("got %s, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve returned %v", err)
			}
			if got != test.want {
				t.Fatalf("got %s, want %s", got, test.want)
			}
		})
	}
}

func TestResolveConcurrency(t *testing.T) {
	tests := []struct {
		name      string
		flagSet   bool
		flagValue int
		raw       string
		want      int
		wantErr   bool
	}{
		{name: "neither flag nor environment", want: purge.DefaultConcurrency},
		{name: "environment alone", raw: "128", want: 128},
		{name: "flag alone", flagSet: true, flagValue: 32, want: 32},
		{name: "flag beats a bad environment value", flagSet: true, flagValue: 32, raw: "lots", want: 32},
		{name: "bad environment value alone", raw: "lots", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolve(envConcurrency, test.flagSet, test.flagValue, test.raw, purge.DefaultConcurrency, strconv.Atoi)
			if test.wantErr {
				if err == nil {
					t.Fatalf("got %d, want an error", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolve returned %v", err)
			}
			if got != test.want {
				t.Fatalf("got %d, want %d", got, test.want)
			}
		})
	}
}
