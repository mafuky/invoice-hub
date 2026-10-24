//go:build lesson

package agent

import (
	"errors"
	"testing"
)

func TestChooseSupplier_ByLineAndExclude(t *testing.T) {
	suppliers := DefaultSuppliers()

	got, err := ChooseSupplier("FLIGHT", nil, suppliers)
	if err != nil || got.Name != "air-a" {
		t.Fatalf("FLIGHT, nothing excluded: got %q err %v, want air-a", got.Name, err)
	}
	got, err = ChooseSupplier("FLIGHT", []string{"air-a"}, suppliers)
	if err != nil || got.Name != "backup" {
		t.Fatalf("FLIGHT, air-a failed before: got %q err %v, want backup", got.Name, err)
	}
	_, err = ChooseSupplier("FLIGHT", []string{"air-a", "backup"}, suppliers)
	if !errors.Is(err, ErrNoSupplier) {
		t.Fatalf("FLIGHT, air-a and backup excluded: err %v, want ErrNoSupplier", err)
	}
}

func TestChooseSupplier_OtherLinesAndUnknownLine(t *testing.T) {
	suppliers := DefaultSuppliers()
	for line, want := range map[string]string{"TRAIN": "rail-a", "HOTEL": "hotel-a"} {
		got, err := ChooseSupplier(line, nil, suppliers)
		if err != nil || got.Name != want {
			t.Fatalf("%s: got %q err %v, want %s", line, got.Name, err, want)
		}
	}
	if _, err := ChooseSupplier("BUS", nil, suppliers); !errors.Is(err, ErrNoSupplier) {
		t.Fatalf("unknown line: err %v, want ErrNoSupplier", err)
	}
}
