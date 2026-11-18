/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

func mustPath(t *testing.T, s string) *gnmipb.Path {
	t.Helper()
	p, err := parsePath(s)
	if err != nil {
		t.Fatalf("parsePath(%q) failed: %v", s, err)
	}
	return p
}

// decode runs encodeNotification and unmarshals the result for inspection.
func decode(t *testing.T, name string, n *gnmipb.Notification) map[string]any {
	t.Helper()
	b, err := encodeNotification(name, n)
	if err != nil {
		t.Fatalf("encodeNotification failed: %v", err)
	}
	if b == nil {
		t.Fatal("encodeNotification returned no data")
	}
	var out map[string]any
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatalf("result is not valid JSON: %v (%s)", err, string(b))
	}
	return out
}

func values(t *testing.T, rec map[string]any) map[string]any {
	t.Helper()
	v, ok := rec[`values`].(map[string]any)
	if !ok {
		t.Fatalf("record has no values object: %#v", rec)
	}
	return v
}

func TestEncodeNotificationScalars(t *testing.T) {
	n := &gnmipb.Notification{
		Timestamp: time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC).UnixNano(),
		Prefix:    mustPath(t, `/interfaces/interface[name=eth0]/state/counters`),
		Update: []*gnmipb.Update{
			{
				Path: mustPath(t, `/in-octets`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: 84224213}},
			},
			{
				Path: mustPath(t, `/oper-status`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_StringVal{StringVal: `UP`}},
			},
			{
				Path: mustPath(t, `/enabled`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_BoolVal{BoolVal: true}},
			},
			{
				Path: mustPath(t, `/in-errors`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_IntVal{IntVal: -3}},
			},
		},
	}

	rec := decode(t, `core-sw1`, n)
	if rec[`target`] != `core-sw1` {
		t.Errorf("target = %v, want core-sw1", rec[`target`])
	}
	if want := `/interfaces/interface[name=eth0]/state/counters`; rec[`prefix`] != want {
		t.Errorf("prefix = %v, want %v", rec[`prefix`], want)
	}

	v := values(t, rec)
	// Keys are relative to the prefix, so a leaf directly under it is bare.
	if got, ok := v[`in-octets`].(float64); !ok || got != 84224213 {
		t.Errorf("in-octets = %#v, want 84224213", v[`in-octets`])
	}
	if v[`oper-status`] != `UP` {
		t.Errorf("oper-status = %#v, want UP", v[`oper-status`])
	}
	if v[`enabled`] != true {
		t.Errorf("enabled = %#v, want true", v[`enabled`])
	}
	if got, ok := v[`in-errors`].(float64); !ok || got != -3 {
		t.Errorf("in-errors = %#v, want -3", v[`in-errors`])
	}
}

func TestEncodeNotificationNestedKeys(t *testing.T) {
	// A leaf several levels below the prefix keeps its relative path, joined
	// with slashes, so it stays distinguishable from a sibling.
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/interfaces/interface[name=eth0]`),
		Update: []*gnmipb.Update{
			{
				Path: mustPath(t, `/state/counters/in-octets`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: 7}},
			},
		},
	}
	v := values(t, decode(t, `d`, n))
	if _, ok := v[`state/counters/in-octets`]; !ok {
		t.Errorf("missing nested key, got %#v", v)
	}
}

func TestEncodeNotificationJSONIETFSubtree(t *testing.T) {
	// JSON_IETF is the common encoding and value.ToScalar would wrap it in a
	// DeprecatedScalar struct, so make sure we decode it into real structure.
	blob := []byte(`{"in-octets":"84224213","admin-status":"UP","sub":{"x":1}}`)
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/interfaces/interface[name=eth0]/state`),
		Update: []*gnmipb.Update{
			{
				Path: mustPath(t, `/counters`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: blob}},
			},
		},
	}
	v := values(t, decode(t, `d`, n))
	sub, ok := v[`counters`].(map[string]any)
	if !ok {
		t.Fatalf("counters is not an object: %#v", v[`counters`])
	}
	if sub[`in-octets`] != `84224213` {
		t.Errorf("in-octets = %#v, want the string 84224213", sub[`in-octets`])
	}
	if sub[`admin-status`] != `UP` {
		t.Errorf("admin-status = %#v, want UP", sub[`admin-status`])
	}
	// Nothing should carry the DeprecatedScalar wrapper's fields.
	if _, bad := sub[`Message`]; bad {
		t.Error("value was wrapped in a DeprecatedScalar")
	}
	if _, ok := sub[`sub`].(map[string]any); !ok {
		t.Errorf("nested object did not survive: %#v", sub[`sub`])
	}
}

func TestEncodeNotificationJSONSubtreeLifted(t *testing.T) {
	// An update whose path is empty covers the prefix itself; its fields get
	// lifted into values rather than buried under a synthetic key.
	blob := []byte(`{"in-octets":1,"out-octets":2}`)
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/interfaces/interface[name=eth0]/state/counters`),
		Update: []*gnmipb.Update{
			{Val: &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: blob}}},
		},
	}
	v := values(t, decode(t, `d`, n))
	if got, ok := v[`in-octets`].(float64); !ok || got != 1 {
		t.Errorf("in-octets = %#v, want 1", v[`in-octets`])
	}
	if got, ok := v[`out-octets`].(float64); !ok || got != 2 {
		t.Errorf("out-octets = %#v, want 2", v[`out-octets`])
	}
	if _, bad := v[`value`]; bad {
		t.Error("subtree was buried under a synthetic key instead of lifted")
	}
}

func TestEncodeNotificationLargeCounter(t *testing.T) {
	// Interface counters are uint64 and routinely exceed 2^53. Decoding a
	// JSON blob through float64 would quietly round them, so the encoder
	// uses json.Number; verify the digits survive the whole trip.
	const big = `18446744073709551615`
	blob := []byte(`{"in-octets":` + big + `}`)
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/c`),
		Update: []*gnmipb.Update{
			{Val: &gnmipb.TypedValue{Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: blob}}},
		},
	}
	b, err := encodeNotification(`d`, n)
	if err != nil {
		t.Fatalf("encodeNotification failed: %v", err)
	}
	var out struct {
		Values struct {
			InOctets json.Number `json:"in-octets"`
		} `json:"values"`
	}
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if out.Values.InOctets.String() != big {
		t.Errorf("in-octets = %s, want %s (precision lost)", out.Values.InOctets, big)
	}

	// A uint64 arriving as a scalar rather than inside JSON must survive too.
	n2 := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/c`),
		Update: []*gnmipb.Update{
			{
				Path: mustPath(t, `/in-octets`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: 18446744073709551615}},
			},
		},
	}
	if b, err = encodeNotification(`d`, n2); err != nil {
		t.Fatalf("encodeNotification failed: %v", err)
	}
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if out.Values.InOctets.String() != big {
		t.Errorf("scalar in-octets = %s, want %s", out.Values.InOctets, big)
	}
}

func TestEncodeNotificationDeletes(t *testing.T) {
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/interfaces`),
		Delete: []*gnmipb.Path{
			mustPath(t, `/interface[name=eth1]`),
		},
	}
	rec := decode(t, `d`, n)
	dels, ok := rec[`deletes`].([]any)
	if !ok || len(dels) != 1 {
		t.Fatalf("deletes = %#v, want one entry", rec[`deletes`])
	}
	// Deletes are relative to the prefix exactly as update keys are, so they
	// are rendered the same way.  Emitting a leading slash here would have a
	// consumer join prefix and delete into "/interfaces//interface[...]".
	if dels[0] != `interface[name=eth1]` {
		t.Errorf("delete = %v, want interface[name=eth1]", dels[0])
	}
	// A delete-only notification still carries no values object.
	if _, present := rec[`values`]; present {
		t.Errorf("values should be omitted when empty, got %#v", rec[`values`])
	}
}

// TestEncodeNotificationDeleteMatchesValueKeys pins the thing that actually
// matters about delete rendering: a path that appears as an update key and as
// a delete in the same shape must come out identically, so a consumer can line
// the two up.
func TestEncodeNotificationDeleteMatchesValueKeys(t *testing.T) {
	path := `/interface[name=eth1]/state`
	upd := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/interfaces`),
		Update: []*gnmipb.Update{{
			Path: mustPath(t, path),
			Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_IntVal{IntVal: 1}},
		}},
	}
	del := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/interfaces`),
		Delete:    []*gnmipb.Path{mustPath(t, path)},
	}

	vals, ok := decode(t, `d`, upd)[`values`].(map[string]any)
	if !ok || len(vals) != 1 {
		t.Fatalf("expected exactly one value, got %#v", vals)
	}
	var key string
	for k := range vals {
		key = k
	}
	dels, ok := decode(t, `d`, del)[`deletes`].([]any)
	if !ok || len(dels) != 1 {
		t.Fatalf("expected exactly one delete, got %#v", dels)
	}
	if dels[0] != key {
		t.Errorf("delete %q and update key %q disagree for the same path", dels[0], key)
	}
}

func TestEncodeNotificationEmpty(t *testing.T) {
	// Nothing to ingest means no entry rather than an empty husk.
	n := &gnmipb.Notification{Timestamp: 1, Prefix: mustPath(t, `/a`)}
	b, err := encodeNotification(`d`, n)
	if err != nil {
		t.Fatalf("encodeNotification failed: %v", err)
	}
	if b != nil {
		t.Errorf("expected no data, got %s", string(b))
	}
}

func TestEncodeNotificationPrefixTarget(t *testing.T) {
	// When the device names itself in the prefix, that wins over our config
	// stanza name.
	p := mustPath(t, `/interfaces`)
	p.Target = `device-reported-name`
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    p,
		Update: []*gnmipb.Update{
			{
				Path: mustPath(t, `/x`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_IntVal{IntVal: 1}},
			},
		},
	}
	rec := decode(t, `config-stanza-name`, n)
	if rec[`target`] != `device-reported-name` {
		t.Errorf("target = %v, want device-reported-name", rec[`target`])
	}
}

func TestEncodeNotificationLeaflist(t *testing.T) {
	lv := &gnmipb.TypedValue{
		Value: &gnmipb.TypedValue_LeaflistVal{
			LeaflistVal: &gnmipb.ScalarArray{
				Element: []*gnmipb.TypedValue{
					{Value: &gnmipb.TypedValue_StringVal{StringVal: `a`}},
					{Value: &gnmipb.TypedValue_StringVal{StringVal: `b`}},
				},
			},
		},
	}
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/a`),
		Update:    []*gnmipb.Update{{Path: mustPath(t, `/list`), Val: lv}},
	}
	v := values(t, decode(t, `d`, n))
	got, ok := v[`list`].([]any)
	if !ok || len(got) != 2 || got[0] != `a` || got[1] != `b` {
		t.Errorf("list = %#v, want [a b]", v[`list`])
	}
}

func TestNotificationTimestamp(t *testing.T) {
	// The device's own sample time is the point of streaming telemetry, so
	// it must land on the entry exactly, nanoseconds included.
	want := time.Date(2026, 11, 16, 10, 0, 0, 123456789, time.UTC)
	n := &gnmipb.Notification{Timestamp: want.UnixNano()}
	got := notificationTS(n).StandardTime().UTC()
	if !got.Equal(want) {
		t.Errorf("timestamp = %v, want %v", got, want)
	}

	// A device that omits the timestamp gets arrival time rather than 1970.
	before := time.Now().Add(-time.Minute)
	got = notificationTS(&gnmipb.Notification{}).StandardTime()
	if got.Before(before) {
		t.Errorf("missing timestamp produced %v, expected roughly now", got)
	}
}

func TestDecodeTypedValueErrors(t *testing.T) {
	// Malformed JSON from a device must surface as an error rather than
	// silently landing as a broken entry.
	bad := &gnmipb.TypedValue{
		Value: &gnmipb.TypedValue_JsonIetfVal{JsonIetfVal: []byte(`{"a":`)},
	}
	if _, err := decodeTypedValue(bad); err == nil {
		t.Error("expected an error for malformed JSON")
	}

	if v, err := decodeTypedValue(nil); err != nil || v != nil {
		t.Errorf("nil typed value = (%#v, %v), want (nil, nil)", v, err)
	}

	// A bad leaf must not take the rest of the sample with it.  The good
	// counters still land and the failure is recorded in the entry.
	good := &gnmipb.TypedValue{Value: &gnmipb.TypedValue_IntVal{IntVal: 42}}
	n := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/a`),
		Update: []*gnmipb.Update{
			{Path: mustPath(t, `/x`), Val: bad},
			{Path: mustPath(t, `/y`), Val: good},
		},
	}
	out := decode(t, `d`, n)
	vals, ok := out[`values`].(map[string]any)
	if !ok {
		t.Fatalf("no values in %#v", out)
	}
	if _, bad := vals[`x`]; bad {
		t.Error("the undecodable leaf should not have been emitted")
	}
	if got := vals[`y`]; got != float64(42) {
		t.Errorf("good leaf y = %#v, want 42", got)
	}
	errs, ok := out[`decode_errors`].([]any)
	if !ok || len(errs) != 1 {
		t.Fatalf("decode_errors = %#v, want one entry", out[`decode_errors`])
	}
	if s, _ := errs[0].(string); !strings.HasPrefix(s, `x: `) {
		t.Errorf("decode_errors[0] = %q, want it to name leaf x", s)
	}

	// A notification whose only update fails still produces an entry, so the
	// failure is visible rather than silently dropped.
	only := &gnmipb.Notification{
		Timestamp: 1,
		Prefix:    mustPath(t, `/a`),
		Update:    []*gnmipb.Update{{Path: mustPath(t, `/x`), Val: bad}},
	}
	b, err := encodeNotification(`d`, only)
	if err != nil {
		t.Fatalf("encodeNotification failed: %v", err)
	}
	if b == nil {
		t.Fatal("a notification of nothing but decode failures was dropped")
	}
}

func TestDecodeTypedValueAscii(t *testing.T) {
	// value.ToScalar rejects ASCII values outright, so we handle them.
	tv := &gnmipb.TypedValue{Value: &gnmipb.TypedValue_AsciiVal{AsciiVal: `hello`}}
	v, err := decodeTypedValue(tv)
	if err != nil {
		t.Fatalf("decodeTypedValue failed: %v", err)
	}
	if v != `hello` {
		t.Errorf("= %#v, want hello", v)
	}
}
