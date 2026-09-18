package purge

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestFailuresKeepsEveryError(t *testing.T) {
	var report failures
	report.add(errors.New("first"))
	report.add(errors.New("second"))

	err := report.err()
	if err == nil {
		t.Fatal("want an error, got nil")
	}
	if got := err.Error(); got != "first\nsecond" {
		t.Errorf("want both errors alone, got %q", got)
	}
}

func TestFailuresCapsTheReport(t *testing.T) {
	var report failures
	for i := range maxReportedErrors + 5 {
		report.add(fmt.Errorf("failure %d", i))
	}

	got := report.err().Error()
	want := fmt.Sprintf("%d documents could not be purged, of which the first %d are:",
		maxReportedErrors+5, maxReportedErrors)
	if !strings.HasPrefix(got, want) {
		t.Errorf("want the count first, got %q", got)
	}
	if strings.Contains(got, fmt.Sprintf("failure %d", maxReportedErrors)) {
		t.Errorf("want only the first %d failures, got %q", maxReportedErrors, got)
	}
}

func TestFailuresNoneIsNil(t *testing.T) {
	var report failures
	if err := report.err(); err != nil {
		t.Errorf("want nil, got %v", err)
	}
}

func TestFailuresCountsFromEveryWorker(t *testing.T) {
	var report failures
	var workers sync.WaitGroup
	for i := range 50 {
		workers.Go(func() {
			report.add(fmt.Errorf("failure %d", i))
		})
	}
	workers.Wait()

	if report.total != 50 {
		t.Errorf("want 50 failures, got %d", report.total)
	}
}
