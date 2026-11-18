/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/gravwell/gravwell/v3/ingest/log"
	"github.com/gravwell/gravwell/v3/ingest/processors"
)

// fakeDevice is a minimal gNMI target: just enough of the Subscribe RPC to
// drive the ingester end to end without hardware.
type fakeDevice struct {
	gnmipb.UnimplementedGNMIServer

	notifications []*gnmipb.Notification
	hold          bool // keep the stream open after the initial dump

	mtx  sync.Mutex
	reqs []*gnmipb.SubscribeRequest
	mds  []metadata.MD
}

func (f *fakeDevice) Subscribe(stream gnmipb.GNMI_SubscribeServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	req, err := stream.Recv()
	if err != nil {
		return err
	}

	f.mtx.Lock()
	f.reqs = append(f.reqs, req)
	f.mds = append(f.mds, md)
	f.mtx.Unlock()

	if err = f.dump(stream); err != nil {
		return err
	}

	if !f.hold {
		return nil // hang up right after the initial dump
	}

	// Stay up, answering polls until the client goes away.
	for {
		req, err := stream.Recv()
		if err != nil {
			return nil
		}
		f.mtx.Lock()
		f.reqs = append(f.reqs, req)
		f.mtx.Unlock()
		if req.GetPoll() != nil {
			if err = f.dump(stream); err != nil {
				return err
			}
		}
	}
}

// dump sends the canned notifications followed by a sync response.
func (f *fakeDevice) dump(stream gnmipb.GNMI_SubscribeServer) error {
	for _, n := range f.notifications {
		if err := stream.Send(&gnmipb.SubscribeResponse{
			Response: &gnmipb.SubscribeResponse_Update{Update: n},
		}); err != nil {
			return err
		}
	}
	return stream.Send(&gnmipb.SubscribeResponse{
		Response: &gnmipb.SubscribeResponse_SyncResponse{SyncResponse: true},
	})
}

func (f *fakeDevice) requests() []*gnmipb.SubscribeRequest {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return append([]*gnmipb.SubscribeRequest(nil), f.reqs...)
}

func (f *fakeDevice) metadatas() []metadata.MD {
	f.mtx.Lock()
	defer f.mtx.Unlock()
	return append([]metadata.MD(nil), f.mds...)
}

// startFakeDevice brings the fake up on a loopback port and tears it down
// when the test finishes.
func startFakeDevice(t *testing.T, dev *fakeDevice) string {
	t.Helper()
	lst, err := net.Listen(`tcp`, `127.0.0.1:0`)
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	gnmipb.RegisterGNMIServer(srv, dev)
	go srv.Serve(lst) //nolint:errcheck
	t.Cleanup(srv.Stop)
	return lst.Addr().String()
}

// capture collects the entries the ingester produces.
type capture struct {
	mtx  sync.Mutex
	ents []*entry.Entry
	got  chan struct{}
}

func newCapture() *capture {
	return &capture{got: make(chan struct{}, 1024)}
}

func (c *capture) add(ents ...*entry.Entry) error {
	c.mtx.Lock()
	c.ents = append(c.ents, ents...)
	c.mtx.Unlock()
	for range ents {
		select {
		case c.got <- struct{}{}:
		default:
		}
	}
	return nil
}

func (c *capture) WriteEntry(ent *entry.Entry) error { return c.add(ent) }
func (c *capture) WriteEntryContext(_ context.Context, ent *entry.Entry) error {
	return c.add(ent)
}
func (c *capture) WriteBatch(ents []*entry.Entry) error { return c.add(ents...) }
func (c *capture) WriteBatchContext(_ context.Context, ents []*entry.Entry) error {
	return c.add(ents...)
}

func (c *capture) entries() []*entry.Entry {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	return append([]*entry.Entry(nil), c.ents...)
}

// wait blocks until n entries have arrived or the test times out.
func (c *capture) wait(t *testing.T, n int) []*entry.Entry {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		if ents := c.entries(); len(ents) >= n {
			return ents
		}
		select {
		case <-c.got:
		case <-deadline:
			t.Fatalf("timed out waiting for %d entries, got %d", n, len(c.entries()))
		}
	}
}

// runTarget wires a target at the fake device and runs it until cleanup.
func runTarget(t *testing.T, tgt *target, cap *capture) *gnmiTarget {
	t.Helper()
	if err := tgt.validate(); err != nil {
		t.Fatalf("target config invalid: %v", err)
	}
	gt, err := newGNMITarget(`core-sw1`, tgt, 0,
		processors.NewProcessorSet(cap), log.NewDiscardLogger())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go gt.run(ctx, &wg)
	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})
	return gt
}

func testNotification(t *testing.T) *gnmipb.Notification {
	t.Helper()
	return &gnmipb.Notification{
		Timestamp: time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC).UnixNano(),
		Prefix:    mustPath(t, `/interfaces/interface[name=eth0]/state/counters`),
		Update: []*gnmipb.Update{
			{
				Path: mustPath(t, `/in-octets`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: 84224213}},
			},
			{
				Path: mustPath(t, `/out-octets`),
				Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: 13371337}},
			},
		},
	}
}

func falsePtr() *bool { b := false; return &b }

// TestSubscribeEndToEnd drives the whole path: dial, authenticate, subscribe,
// receive a notification, and produce an entry.
func TestSubscribeEndToEnd(t *testing.T) {
	dev := &fakeDevice{
		notifications: []*gnmipb.Notification{testNotification(t)},
		hold:          true,
	}
	addr := startFakeDevice(t, dev)

	cap := newCapture()
	runTarget(t, &target{
		Tag_Name:       `gnmi`,
		Host:           addr,
		Username:       `admin`,
		Password:       `secret`,
		Use_TLS:        falsePtr(),
		Subscribe_Path: []string{`/interfaces/interface/state/counters`},
	}, cap)

	ents := cap.wait(t, 1)
	ent := ents[0]

	// The device's own sample timestamp must land on the entry.
	want := time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC)
	if got := ent.TS.StandardTime().UTC(); !got.Equal(want) {
		t.Errorf("entry TS = %v, want %v", got, want)
	}
	if !ent.SRC.Equal(net.ParseIP(`127.0.0.1`)) {
		t.Errorf("entry SRC = %v, want 127.0.0.1", ent.SRC)
	}

	var rec map[string]any
	if err := json.Unmarshal(ent.Data, &rec); err != nil {
		t.Fatalf("entry is not valid JSON: %v (%s)", err, string(ent.Data))
	}
	if rec[`target`] != `core-sw1` {
		t.Errorf("target = %v, want core-sw1", rec[`target`])
	}
	vals, ok := rec[`values`].(map[string]any)
	if !ok {
		t.Fatalf("no values in entry: %s", string(ent.Data))
	}
	if got, ok := vals[`in-octets`].(float64); !ok || got != 84224213 {
		t.Errorf("in-octets = %#v, want 84224213", vals[`in-octets`])
	}
	if got, ok := vals[`out-octets`].(float64); !ok || got != 13371337 {
		t.Errorf("out-octets = %#v, want 13371337", vals[`out-octets`])
	}
}

// TestSubscribeCredentials checks that the username and password reach the
// device as per-RPC metadata, which is how every vendor expects them.
func TestSubscribeCredentials(t *testing.T) {
	dev := &fakeDevice{
		notifications: []*gnmipb.Notification{testNotification(t)},
		hold:          true,
	}
	addr := startFakeDevice(t, dev)

	cap := newCapture()
	runTarget(t, &target{
		Tag_Name:       `gnmi`,
		Host:           addr,
		Username:       `admin`,
		Password:       `secret`,
		Use_TLS:        falsePtr(),
		Subscribe_Path: []string{`/interfaces/interface/state/counters`},
	}, cap)
	cap.wait(t, 1)

	mds := dev.metadatas()
	if len(mds) == 0 {
		t.Fatal("device saw no metadata")
	}
	if got := mds[0].Get(`username`); len(got) != 1 || got[0] != `admin` {
		t.Errorf("username metadata = %v, want [admin]", got)
	}
	if got := mds[0].Get(`password`); len(got) != 1 || got[0] != `secret` {
		t.Errorf("password metadata = %v, want [secret]", got)
	}
}

// TestSubscribeRequestShape checks the subscription we actually put on the
// wire, since a malformed one is silently ignored by many devices.
func TestSubscribeRequestShape(t *testing.T) {
	dev := &fakeDevice{hold: true}
	addr := startFakeDevice(t, dev)

	cap := newCapture()
	runTarget(t, &target{
		Tag_Name:        `gnmi`,
		Host:            addr,
		Use_TLS:         falsePtr(),
		Subscribe_Path:  []string{`/interfaces/interface/state/counters`, `/system/memory/state`},
		Stream_Mode:     `sample`,
		Sample_Interval: `30s`,
		Origin:          `openconfig`,
		Encoding:        `json_ietf`,
	}, cap)

	// No entries will arrive, so poll for the request instead.
	deadline := time.After(10 * time.Second)
	var reqs []*gnmipb.SubscribeRequest
	for len(reqs) == 0 {
		select {
		case <-deadline:
			t.Fatal("device never received a subscribe request")
		case <-time.After(10 * time.Millisecond):
		}
		reqs = dev.requests()
	}

	sl := reqs[0].GetSubscribe()
	if sl == nil {
		t.Fatalf("request was not a SubscriptionList: %#v", reqs[0])
	}
	if sl.GetMode() != gnmipb.SubscriptionList_STREAM {
		t.Errorf("mode = %v, want STREAM", sl.GetMode())
	}
	if sl.GetEncoding() != gnmipb.Encoding_JSON_IETF {
		t.Errorf("encoding = %v, want JSON_IETF", sl.GetEncoding())
	}
	subs := sl.GetSubscription()
	if len(subs) != 2 {
		t.Fatalf("got %d subscriptions, want 2", len(subs))
	}
	for i, s := range subs {
		if s.GetMode() != gnmipb.SubscriptionMode_SAMPLE {
			t.Errorf("subscription %d mode = %v, want SAMPLE", i, s.GetMode())
		}
		// Junos rejects a request carrying suppress_redundant at all, so an
		// unconfigured subscription must leave these at their zero values,
		// which proto3 then keeps off the wire entirely.
		if s.GetSuppressRedundant() {
			t.Errorf("subscription %d set suppress_redundant without being asked", i)
		}
		if s.GetHeartbeatInterval() != 0 {
			t.Errorf("subscription %d set heartbeat_interval without being asked", i)
		}
		if got := s.GetSampleInterval(); got != uint64(30*time.Second) {
			t.Errorf("subscription %d sample interval = %d, want %d", i, got, uint64(30*time.Second))
		}
		if got := s.GetPath().GetOrigin(); got != `openconfig` {
			t.Errorf("subscription %d origin = %q, want openconfig", i, got)
		}
	}
	if got := formatPath(subs[0].GetPath()); got != `openconfig:/interfaces/interface/state/counters` {
		t.Errorf("subscription 0 path = %q", got)
	}
}

// TestSubscribeReconnects makes sure a dropped stream is picked back up.
// Devices drop streams routinely, so this is the normal path, not an edge.
func TestSubscribeReconnects(t *testing.T) {
	// hold=false means the device hangs up right after the initial dump.
	dev := &fakeDevice{
		notifications: []*gnmipb.Notification{testNotification(t)},
		hold:          false,
	}
	addr := startFakeDevice(t, dev)

	cap := newCapture()
	runTarget(t, &target{
		Tag_Name:       `gnmi`,
		Host:           addr,
		Use_TLS:        falsePtr(),
		Subscribe_Path: []string{`/interfaces/interface/state/counters`},
	}, cap)

	// Two entries means we came back after the first stream ended.
	ents := cap.wait(t, 2)
	if len(ents) < 2 {
		t.Fatalf("got %d entries, want at least 2", len(ents))
	}
	if len(dev.requests()) < 2 {
		t.Errorf("device saw %d subscribe requests, want at least 2", len(dev.requests()))
	}
}

// TestSubscribeJunosNative covers a Juniper style target: a native sensor
// path with no origin, PROTO encoding, and values arriving as typed scalars
// rather than a JSON blob.
func TestSubscribeJunosNative(t *testing.T) {
	const sensor = `/junos/security/spu/flow/statistics/session`
	dev := &fakeDevice{
		hold: true,
		notifications: []*gnmipb.Notification{
			{
				Timestamp: time.Date(2026, 11, 16, 10, 0, 0, 0, time.UTC).UnixNano(),
				Prefix:    mustPath(t, sensor),
				Update: []*gnmipb.Update{
					{
						Path: mustPath(t, `/active-sessions`),
						Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_UintVal{UintVal: 412233}},
					},
					{
						Path: mustPath(t, `/spu-name`),
						Val:  &gnmipb.TypedValue{Value: &gnmipb.TypedValue_StringVal{StringVal: `node0:fpc1:pic0`}},
					},
				},
			},
		},
	}
	addr := startFakeDevice(t, dev)

	cap := newCapture()
	runTarget(t, &target{
		Tag_Name:        `junos`,
		Host:            addr,
		Username:        `admin`,
		Password:        `secret`,
		Use_TLS:         falsePtr(),
		Subscribe_Path:  []string{sensor},
		Encoding:        `proto`,
		Stream_Mode:     `sample`,
		Sample_Interval: `30s`,
	}, cap)

	ents := cap.wait(t, 1)

	reqs := dev.requests()
	if got := reqs[0].GetSubscribe().GetEncoding(); got != gnmipb.Encoding_PROTO {
		t.Errorf("encoding = %v, want PROTO", got)
	}
	// A native sensor path carries no origin; we must not invent one.
	if got := reqs[0].GetSubscribe().GetSubscription()[0].GetPath().GetOrigin(); got != `` {
		t.Errorf("origin = %q, want empty for a native sensor path", got)
	}
	if got := formatPath(reqs[0].GetSubscribe().GetSubscription()[0].GetPath()); got != sensor {
		t.Errorf("path = %q, want %q", got, sensor)
	}

	var rec map[string]any
	if err := json.Unmarshal(ents[0].Data, &rec); err != nil {
		t.Fatalf("entry is not valid JSON: %v", err)
	}
	if rec[`prefix`] != sensor {
		t.Errorf("prefix = %v, want %v", rec[`prefix`], sensor)
	}
	vals, ok := rec[`values`].(map[string]any)
	if !ok {
		t.Fatalf("no values: %s", string(ents[0].Data))
	}
	if got, ok := vals[`active-sessions`].(float64); !ok || got != 412233 {
		t.Errorf("active-sessions = %#v, want 412233", vals[`active-sessions`])
	}
	if vals[`spu-name`] != `node0:fpc1:pic0` {
		t.Errorf("spu-name = %#v, want node0:fpc1:pic0", vals[`spu-name`])
	}
}

// TestSubscribeOnceMode covers ONCE, which is a poll dressed up as a stream:
// the device hangs up after one dump and we re-subscribe on the sample
// interval rather than on the error backoff.
func TestSubscribeOnceMode(t *testing.T) {
	dev := &fakeDevice{
		notifications: []*gnmipb.Notification{testNotification(t)},
		hold:          false,
	}
	addr := startFakeDevice(t, dev)

	cap := newCapture()
	runTarget(t, &target{
		Tag_Name:        `gnmi`,
		Host:            addr,
		Use_TLS:         falsePtr(),
		Subscribe_Path:  []string{`/interfaces/interface/state/counters`},
		Mode:            `once`,
		Sample_Interval: `100ms`,
	}, cap)

	cap.wait(t, 2) // re-subscribed at least once

	reqs := dev.requests()
	if len(reqs) < 2 {
		t.Fatalf("device saw %d requests, want at least 2", len(reqs))
	}
	if got := reqs[0].GetSubscribe().GetMode(); got != gnmipb.SubscriptionList_ONCE {
		t.Errorf("mode = %v, want ONCE", got)
	}
	// The per-subscription stream knobs mean nothing outside STREAM mode and
	// some targets reject a request that carries them anyway.
	for i, s := range reqs[0].GetSubscribe().GetSubscription() {
		if s.GetSampleInterval() != 0 {
			t.Errorf("subscription %d carries a sample interval in ONCE mode", i)
		}
		if s.GetMode() != gnmipb.SubscriptionMode_TARGET_DEFINED {
			t.Errorf("subscription %d carries stream mode %v in ONCE mode", i, s.GetMode())
		}
	}
}

// TestSubscribePollMode covers POLL, where we hold one stream open and ask
// for a fresh dump on each interval.
func TestSubscribePollMode(t *testing.T) {
	dev := &fakeDevice{
		notifications: []*gnmipb.Notification{testNotification(t)},
		hold:          true,
	}
	addr := startFakeDevice(t, dev)

	cap := newCapture()
	runTarget(t, &target{
		Tag_Name:        `gnmi`,
		Host:            addr,
		Use_TLS:         falsePtr(),
		Subscribe_Path:  []string{`/interfaces/interface/state/counters`},
		Mode:            `poll`,
		Sample_Interval: `100ms`,
	}, cap)

	// Initial dump plus at least one poll response.
	cap.wait(t, 2)

	reqs := dev.requests()
	if len(reqs) < 2 {
		t.Fatalf("device saw %d requests, want at least 2", len(reqs))
	}
	if got := reqs[0].GetSubscribe().GetMode(); got != gnmipb.SubscriptionList_POLL {
		t.Errorf("mode = %v, want POLL", got)
	}
	var polls int
	for _, r := range reqs {
		if r.GetPoll() != nil {
			polls++
		}
	}
	if polls == 0 {
		t.Error("device never received a poll request")
	}
}

// TestSubscribeShutdown makes sure cancelling the context actually stops the
// collection goroutine rather than leaking it.
func TestSubscribeShutdown(t *testing.T) {
	dev := &fakeDevice{
		notifications: []*gnmipb.Notification{testNotification(t)},
		hold:          true,
	}
	addr := startFakeDevice(t, dev)

	tgt := &target{
		Tag_Name:       `gnmi`,
		Host:           addr,
		Use_TLS:        falsePtr(),
		Subscribe_Path: []string{`/interfaces/interface/state/counters`},
	}
	if err := tgt.validate(); err != nil {
		t.Fatal(err)
	}
	cap := newCapture()
	gt, err := newGNMITarget(`core-sw1`, tgt, 0,
		processors.NewProcessorSet(cap), log.NewDiscardLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go gt.run(ctx, &wg)

	cap.wait(t, 1)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("collection goroutine did not stop after cancellation")
	}
}

// TestSubscribeUnreachable makes sure an unreachable device is survivable:
// the goroutine retries rather than spinning or dying.
func TestSubscribeUnreachable(t *testing.T) {
	// Grab a port and immediately release it so nothing is listening.
	lst, err := net.Listen(`tcp`, `127.0.0.1:0`)
	if err != nil {
		t.Fatal(err)
	}
	addr := lst.Addr().String()
	lst.Close()

	tgt := &target{
		Tag_Name:       `gnmi`,
		Host:           addr,
		Use_TLS:        falsePtr(),
		Subscribe_Path: []string{`/a/b`},
	}
	if err = tgt.validate(); err != nil {
		t.Fatal(err)
	}
	cap := newCapture()
	gt, err := newGNMITarget(`down`, tgt, 0,
		processors.NewProcessorSet(cap), log.NewDiscardLogger())
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go gt.run(ctx, &wg)

	time.Sleep(500 * time.Millisecond)
	cancel()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("goroutine did not stop after cancellation")
	}
	if n := len(cap.entries()); n != 0 {
		t.Errorf("got %d entries from an unreachable device, want 0", n)
	}
}
