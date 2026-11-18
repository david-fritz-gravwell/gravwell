/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"testing"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

func TestParsePath(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		origin string
		elems  []*gnmipb.PathElem
	}{
		{
			name: "simple",
			in:   `/interfaces/interface/state/counters`,
			elems: []*gnmipb.PathElem{
				{Name: `interfaces`}, {Name: `interface`},
				{Name: `state`}, {Name: `counters`},
			},
		},
		{
			name: "single key",
			in:   `/interfaces/interface[name=eth0]/state`,
			elems: []*gnmipb.PathElem{
				{Name: `interfaces`},
				{Name: `interface`, Key: map[string]string{`name`: `eth0`}},
				{Name: `state`},
			},
		},
		{
			name: "multiple keys",
			in:   `/network-instances/network-instance[name=default][type=L3VRF]/protocols`,
			elems: []*gnmipb.PathElem{
				{Name: `network-instances`},
				{Name: `network-instance`, Key: map[string]string{`name`: `default`, `type`: `L3VRF`}},
				{Name: `protocols`},
			},
		},
		{
			name:   "origin",
			in:     `openconfig:/interfaces/interface/state`,
			origin: `openconfig`,
			elems: []*gnmipb.PathElem{
				{Name: `interfaces`}, {Name: `interface`}, {Name: `state`},
			},
		},
		{
			name:   "empty origin path",
			in:     `openconfig:/`,
			origin: `openconfig`,
		},
		{
			name:  "root",
			in:    `/`,
			elems: nil,
		},
		{
			// Junos native sensor, renamed from .../flow/usage in 24.2R1.
			// Vendor native paths carry no origin and no keys.
			name: "junos native sensor",
			in:   `/junos/security/spu/flow/statistics/session`,
			elems: []*gnmipb.PathElem{
				{Name: `junos`}, {Name: `security`}, {Name: `spu`},
				{Name: `flow`}, {Name: `statistics`}, {Name: `session`},
			},
		},
		{
			name: "escaped slash in key value",
			in:   `/acl/acl-set[name=a\/b]/state`,
			elems: []*gnmipb.PathElem{
				{Name: `acl`},
				{Name: `acl-set`, Key: map[string]string{`name`: `a/b`}},
				{Name: `state`},
			},
		},
		{
			name: "escaped bracket in key value",
			in:   `/a/b[k=v\]x]/c`,
			elems: []*gnmipb.PathElem{
				{Name: `a`},
				{Name: `b`, Key: map[string]string{`k`: `v]x`}},
				{Name: `c`},
			},
		},
		{
			name: "escaped equals in key value",
			in:   `/a/b[k=v\=x]`,
			elems: []*gnmipb.PathElem{
				{Name: `a`},
				{Name: `b`, Key: map[string]string{`k`: `v=x`}},
			},
		},
		{
			name: "key value with colon",
			in:   `/a/b[addr=2001:db8::1]/c`,
			elems: []*gnmipb.PathElem{
				{Name: `a`},
				{Name: `b`, Key: map[string]string{`addr`: `2001:db8::1`}},
				{Name: `c`},
			},
		},
		{
			name: "empty key value",
			in:   `/a/b[k=]`,
			elems: []*gnmipb.PathElem{
				{Name: `a`},
				{Name: `b`, Key: map[string]string{`k`: ``}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := parsePath(tt.in)
			if err != nil {
				t.Fatalf("parsePath(%q) failed: %v", tt.in, err)
			}
			if p.GetOrigin() != tt.origin {
				t.Errorf("origin = %q, want %q", p.GetOrigin(), tt.origin)
			}
			if len(p.GetElem()) != len(tt.elems) {
				t.Fatalf("got %d elements, want %d: %#v", len(p.GetElem()), len(tt.elems), p.GetElem())
			}
			for i, want := range tt.elems {
				got := p.GetElem()[i]
				if got.GetName() != want.GetName() {
					t.Errorf("element %d name = %q, want %q", i, got.GetName(), want.GetName())
				}
				if len(got.GetKey()) != len(want.GetKey()) {
					t.Fatalf("element %d has %d keys, want %d", i, len(got.GetKey()), len(want.GetKey()))
				}
				for k, v := range want.GetKey() {
					if gv, ok := got.GetKey()[k]; !ok {
						t.Errorf("element %d missing key %q", i, k)
					} else if gv != v {
						t.Errorf("element %d key %q = %q, want %q", i, k, gv, v)
					}
				}
			}
		})
	}
}

func TestParsePathErrors(t *testing.T) {
	// Every one of these is a path we must reject at config validation time
	// rather than at 3am when a subscription silently matches nothing.
	bad := []string{
		``,
		`   `,
		`interfaces/interface`,   // no leading slash
		`/interfaces//interface`, // empty element
		`/interfaces/interface/`, // trailing slash
		`/a/b[k]`,                // key with no =
		`/a/b[=v]`,               // key with no name
		`/a/b[k=v`,               // unterminated bracket
		`/a/b[k=v]x`,             // garbage after predicate
		`/a/b[k=v][k=w]`,         // duplicate key
		`/a/b\`,                  // dangling escape
		`:/a/b`,                  // empty origin
	}
	for _, in := range bad {
		t.Run(in, func(t *testing.T) {
			if p, err := parsePath(in); err == nil {
				t.Errorf("parsePath(%q) should have failed, got %#v", in, p)
			}
		})
	}
}

func TestFormatPathRoundTrip(t *testing.T) {
	paths := []string{
		`/interfaces/interface/state/counters`,
		`/interfaces/interface[name=eth0]/state`,
		`/network-instances/network-instance[name=default][type=L3VRF]/protocols`,
		`openconfig:/interfaces/interface/state`,
		`/acl/acl-set[name=a\/b]/state`,
		`/a/b[k=v\]x]/c`,
		`/a/b[k=v\=x]`,
		`/`,
	}
	for _, in := range paths {
		t.Run(in, func(t *testing.T) {
			p, err := parsePath(in)
			if err != nil {
				t.Fatalf("parsePath(%q) failed: %v", in, err)
			}
			out := formatPath(p)
			// Re-parse the rendered form and make sure it is the same path.
			p2, err := parsePath(out)
			if err != nil {
				t.Fatalf("parsePath(formatPath(%q)) = parsePath(%q) failed: %v", in, out, err)
			}
			if got := formatPath(p2); got != out {
				t.Errorf("round trip unstable: %q -> %q -> %q", in, out, got)
			}
		})
	}
}

func TestFormatPathEscapes(t *testing.T) {
	// A colon only means "origin" ahead of the first slash, but we escape it
	// anyway so the rendered form can never be misread.
	p := &gnmipb.Path{
		Elem: []*gnmipb.PathElem{
			{Name: `a`},
			{Name: `b`, Key: map[string]string{`addr`: `2001:db8::1`}},
		},
	}
	out := formatPath(p)
	p2, err := parsePath(out)
	if err != nil {
		t.Fatalf("parsePath(%q) failed: %v", out, err)
	}
	if got := p2.GetElem()[1].GetKey()[`addr`]; got != `2001:db8::1` {
		t.Errorf("key value = %q, want %q", got, `2001:db8::1`)
	}
	if p2.GetOrigin() != `` {
		t.Errorf("origin = %q, want empty", p2.GetOrigin())
	}
}

func TestFormatElemsKeyOrderStable(t *testing.T) {
	// Go randomizes map iteration; entry keys must not change shape between
	// samples or every downstream extraction breaks intermittently.
	elems := []*gnmipb.PathElem{
		{Name: `x`, Key: map[string]string{`c`: `3`, `a`: `1`, `b`: `2`}},
	}
	want := `x[a=1][b=2][c=3]`
	for range 32 {
		if got := formatElems(elems); got != want {
			t.Fatalf("formatElems = %q, want %q", got, want)
		}
	}
}

func TestFormatElemsRelative(t *testing.T) {
	// Values keys are rendered relative to the prefix, with no leading slash.
	elems := []*gnmipb.PathElem{{Name: `in-octets`}}
	if got := formatElems(elems); got != `in-octets` {
		t.Errorf("formatElems = %q, want %q", got, `in-octets`)
	}
	nested := []*gnmipb.PathElem{{Name: `state`}, {Name: `in-octets`}}
	if got := formatElems(nested); got != `state/in-octets` {
		t.Errorf("formatElems = %q, want %q", got, `state/in-octets`)
	}
	if got := formatElems(nil); got != `` {
		t.Errorf("formatElems(nil) = %q, want empty", got)
	}
}
