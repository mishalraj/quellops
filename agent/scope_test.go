//go:build !legacy

package main

import (
	"reflect"
	"testing"
)

func TestParseNamespaces(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		{"", []string{}},
		{"production, checkout,production", []string{"checkout", "production"}},
		{"production,*", []string{"*"}},
	}
	for _, test := range tests {
		if got := parseNamespaces(test.input); !reflect.DeepEqual(got, test.want) {
			t.Fatalf("parseNamespaces(%q) = %#v, want %#v", test.input, got, test.want)
		}
	}
}
