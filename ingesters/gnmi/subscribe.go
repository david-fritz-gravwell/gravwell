/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"sync"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
	"github.com/openconfig/gnmi/value"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"

	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/gravwell/gravwell/v3/ingest/log"
	"github.com/gravwell/gravwell/v3/ingest/processors"
)

const (
	minBackoff   = time.Second
	maxBackoff   = 30 * time.Second
	backoffReset = 2 * time.Minute

	// A clean close only counts as "the device recycled its stream" if the
	// session actually ran for a moment.  Anything shorter is a device
	// hanging up on us, which must still back off rather than become a
	// reconnect hot loop.
	minSessionLife = 5 * time.Second
)

// gnmiTarget holds everything one device's collection goroutine needs.
type gnmiTarget struct {
	name string
	cfg  *target
	tag  entry.EntryTag
	proc *processors.ProcessorSet
	lg   *log.Logger

	dial string
	src  net.IP
}

func newGNMITarget(name string, cfg *target, tag entry.EntryTag, proc *processors.ProcessorSet, lg *log.Logger) (*gnmiTarget, error) {
	dial, err := cfg.dialTarget()
	if err != nil {
		return nil, err
	}
	g := &gnmiTarget{
		name: name,
		cfg:  cfg,
		tag:  tag,
		proc: proc,
		lg:   lg,
		dial: dial,
	}
	if cfg.Source_Override != `` {
		g.src = net.ParseIP(cfg.Source_Override)
	}
	return g, nil
}

// run maintains a subscription to the device until the context is cancelled.
// Devices drop streams as a matter of course (reboots, process restarts, ACL
// changes), so reconnecting is the normal path rather than an error path.
func (g *gnmiTarget) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	backoff := minBackoff
	for ctx.Err() == nil {
		started := time.Now()
		err := g.session(ctx)
		if ctx.Err() != nil {
			return
		}

		// A session that stayed up a good while is evidence the device is
		// healthy, so do not punish the next reconnect with a long delay.
		lived := time.Since(started)
		if lived >= backoffReset {
			backoff = minBackoff
		}

		grow := func() time.Duration {
			wait := backoff
			if backoff < maxBackoff {
				if backoff *= 2; backoff > maxBackoff {
					backoff = maxBackoff
				}
			}
			return wait
		}

		var wait time.Duration
		switch {
		case err != nil:
			wait = grow()
			g.lg.Error("gnmi session failed",
				log.KV("target", g.name), log.KV("host", g.dial),
				log.KV("retry-in", wait), log.KVErr(err))
		case g.cfg.listMode() == gnmipb.SubscriptionList_ONCE:
			// ONCE is a request/response poll dressed up as a stream; the
			// device closes after one dump, so pace re-subscribes off the
			// sample interval rather than the error backoff.
			backoff = minBackoff
			wait = g.cfg.sampleInterval()
		case lived < minSessionLife:
			// Clean, but immediate.  The device is refusing the subscription
			// without saying so; treat it like a failure so we do not spin.
			wait = grow()
			g.lg.Warn("gnmi stream closed immediately, reconnecting",
				log.KV("target", g.name), log.KV("host", g.dial),
				log.KV("lived", lived), log.KV("retry-in", wait))
		default:
			// A clean close after real work is the normal path, not a
			// failure: devices cycle streams for connection age limits,
			// subscription duration caps and agent restarts.  Backing off
			// here would turn a device that recycles every minute into a
			// permanent duty cycle of collection and silence.
			backoff = minBackoff
			wait = minBackoff
			g.lg.Info("gnmi stream closed by device, reconnecting",
				log.KV("target", g.name), log.KV("host", g.dial),
				log.KV("retry-in", wait))
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// session dials the device, opens one Subscribe stream and pumps it until it
// ends. A nil return means the stream ended without an error.
func (g *gnmiTarget) session(ctx context.Context) error {
	opts, err := g.dialOptions()
	if err != nil {
		return err
	}
	conn, err := grpc.NewClient(g.dial, opts...)
	if err != nil {
		return fmt.Errorf("failed to create client for %s: %w", g.dial, err)
	}
	defer conn.Close()

	// Resolve on each session so that a device moving address is picked up.
	g.resolveSource(ctx)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if g.cfg.Username != `` || g.cfg.Password != `` {
		// gNMI carries credentials as per-RPC metadata, which is what Cisco,
		// Juniper, Arista and Nokia all expect.
		ctx = metadata.AppendToOutgoingContext(ctx,
			`username`, g.cfg.Username, `password`, g.cfg.Password)
	}

	stream, err := gnmipb.NewGNMIClient(conn).Subscribe(ctx)
	if err != nil {
		return fmt.Errorf("failed to open subscribe stream: %w", err)
	}

	req, err := g.subscribeRequest()
	if err != nil {
		return err
	}
	if err = stream.Send(req); err != nil {
		return fmt.Errorf("failed to send subscribe request: %w", err)
	}

	if g.cfg.listMode() == gnmipb.SubscriptionList_POLL {
		var pollWG sync.WaitGroup
		pollWG.Go(func() {
			g.poll(ctx, cancel, stream)
		})
		// Make sure the poller is gone before the stream is torn down.
		defer pollWG.Wait()
		defer cancel()
	}

	return g.pump(ctx, stream)
}

// pump reads the subscribe stream until it ends.
func (g *gnmiTarget) pump(ctx context.Context, stream grpc.BidiStreamingClient[gnmipb.SubscribeRequest, gnmipb.SubscribeResponse]) error {
	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || ctx.Err() != nil {
				return nil // orderly end of stream or our own shutdown
			}
			return err
		}
		switch r := resp.GetResponse().(type) {
		case *gnmipb.SubscribeResponse_Update:
			g.handleNotification(ctx, r.Update)
		case *gnmipb.SubscribeResponse_SyncResponse:
			g.lg.Debug("gnmi sync response", log.KV("target", g.name))
		case *gnmipb.SubscribeResponse_Error:
			// Deprecated in the spec, but older implementations still send
			// it and dropping it on the floor would hide real failures.
			//lint:ignore SA1019 deprecated upstream, still emitted by deployed devices
			se := r.Error
			g.lg.Error("gnmi stream error response",
				log.KV("target", g.name),
				log.KV("code", se.GetCode()),
				log.KV("message", se.GetMessage()))
		}
	}
}

// poll drives POLL mode by asking for a fresh dump on each interval.
//
// It owns the session's cancel: if polling stops, the session has to stop too.
// Otherwise pump would stay parked in Recv on a stream nobody is driving any
// more and the target would go silent while still looking healthy.
func (g *gnmiTarget) poll(ctx context.Context, cancel context.CancelFunc, stream grpc.BidiStreamingClient[gnmipb.SubscribeRequest, gnmipb.SubscribeResponse]) {
	defer cancel()
	tkr := time.NewTicker(g.cfg.sampleInterval())
	defer tkr.Stop()
	req := &gnmipb.SubscribeRequest{
		Request: &gnmipb.SubscribeRequest_Poll{Poll: &gnmipb.Poll{}},
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-tkr.C:
			if err := stream.Send(req); err != nil {
				if ctx.Err() == nil {
					g.lg.Error("gnmi poll failed",
						log.KV("target", g.name), log.KVErr(err))
				}
				return
			}
		}
	}
}

func (g *gnmiTarget) dialOptions() ([]grpc.DialOption, error) {
	// gRPC caps received messages at 4MB by default, which a ONCE dump or a
	// large JSON_IETF subtree on a populated chassis blows straight through.
	// That failure is not transient: the retry gets the same oversized
	// message and the target wedges, so the cap is raised and configurable.
	opts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(g.cfg.maxMessageSize())),
	}
	// Keepalive pings are enforced by the far end.  grpc-go servers default
	// to refusing anything faster than one ping every 5 minutes and send
	// GOAWAY/ENHANCE_YOUR_CALM after three violations, and the strike count
	// only resets when the server sends data.  A quiet ON_CHANGE stream is
	// therefore exactly the case that gets its connection killed, so the
	// default is conservative and operators whose devices permit faster
	// pings can lower it.
	if ka := g.cfg.keepaliveInterval(); ka > 0 {
		opts = append(opts, grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:    ka,
			Timeout: g.cfg.keepaliveTimeout(),
		}))
	}
	if g.cfg.useTLS() {
		tcfg, err := g.cfg.tlsConfig()
		if err != nil {
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tcfg)))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}
	return opts, nil
}

// resolveSource works out the IP to stamp on entries. An explicit
// Source-Override always wins; otherwise we use the device address.
//
// The lookup takes a context because net.LookupIP ignores cancellation, and a
// hung resolver would otherwise hold up shutdown for as long as it takes the
// system resolver to give up.
func (g *gnmiTarget) resolveSource(ctx context.Context) {
	if g.cfg.Source_Override != `` {
		return
	}
	host, _, err := net.SplitHostPort(g.dial)
	if err != nil {
		return
	}
	if ip := net.ParseIP(host); ip != nil {
		g.src = ip
		return
	}
	if addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host); err == nil && len(addrs) > 0 {
		g.src = addrs[0].IP
	}
}

func (g *gnmiTarget) subscribeRequest() (*gnmipb.SubscribeRequest, error) {
	paths, err := g.cfg.subscriptionPaths()
	if err != nil {
		return nil, err
	}
	mode := g.cfg.listMode()
	subs := make([]*gnmipb.Subscription, 0, len(paths))
	for _, p := range paths {
		s := &gnmipb.Subscription{Path: p}
		// The per-subscription knobs only mean anything in STREAM mode;
		// sending them otherwise makes some targets reject the request.
		if mode == gnmipb.SubscriptionList_STREAM {
			s.Mode = g.cfg.streamMode()
			// Only populate these when they were actually asked for. Junos
			// rejects a SubscribeRequest that carries suppress_redundant at
			// all, and proto3 leaves zero values off the wire, so an
			// unconfigured subscription stays acceptable everywhere.
			if g.cfg.Suppress_Redundant {
				s.SuppressRedundant = true
			}
			if hb := g.cfg.heartbeatInterval(); hb > 0 {
				s.HeartbeatInterval = uint64(hb.Nanoseconds())
			}
			if s.Mode == gnmipb.SubscriptionMode_SAMPLE {
				s.SampleInterval = uint64(g.cfg.sampleInterval().Nanoseconds())
			}
		}
		subs = append(subs, s)
	}
	return &gnmipb.SubscribeRequest{
		Request: &gnmipb.SubscribeRequest_Subscribe{
			Subscribe: &gnmipb.SubscriptionList{
				Subscription: subs,
				Mode:         mode,
				Encoding:     g.cfg.encoding(),
				UpdatesOnly:  g.cfg.Updates_Only,
			},
		},
	}, nil
}

func (g *gnmiTarget) handleNotification(ctx context.Context, n *gnmipb.Notification) {
	if n == nil {
		return
	}
	data, err := encodeNotification(g.name, n)
	if err != nil {
		g.lg.Warn("failed to encode gnmi notification",
			log.KV("target", g.name), log.KVErr(err))
		return
	} else if data == nil {
		return // nothing in it
	}
	ent := &entry.Entry{
		TS:   notificationTS(n),
		SRC:  g.src,
		Tag:  g.tag,
		Data: data,
	}
	if err = g.proc.ProcessContext(ent, ctx); err != nil && ctx.Err() == nil {
		g.lg.Error("failed to process gnmi entry",
			log.KV("target", g.name), log.KVErr(err))
	}
}

// notificationTS uses the device's own sample timestamp rather than arrival
// time, which is the whole point of streaming telemetry. gNMI timestamps are
// nanoseconds since the epoch; a device that omits one gets arrival time.
func notificationTS(n *gnmipb.Notification) entry.Timestamp {
	ts := n.GetTimestamp()
	if ts <= 0 {
		return entry.Now()
	}
	return entry.UnixTime(ts/int64(time.Second), ts%int64(time.Second))
}

// notificationRecord is the shape of one ingested entry. A gNMI notification
// is a single timestamped sample covering several leaves, so it maps to one
// entry rather than one entry per leaf.
type notificationRecord struct {
	Target  string         `json:"target"`
	Prefix  string         `json:"prefix,omitempty"`
	Values  map[string]any `json:"values,omitempty"`
	Deletes []string       `json:"deletes,omitempty"`
	Errors  []string       `json:"decode_errors,omitempty"`
}

// encodeNotification renders a notification as the JSON payload of an entry.
// It returns nil when the notification carries nothing worth ingesting.
func encodeNotification(targetName string, n *gnmipb.Notification) ([]byte, error) {
	rec := notificationRecord{Target: targetName}
	if p := n.GetPrefix(); p != nil && (len(p.GetElem()) != 0 || p.GetOrigin() != ``) {
		rec.Prefix = formatPath(p)
	}
	// A prefix may name the device it came from, which is more trustworthy
	// than our config stanza name, so prefer it when present.
	if t := n.GetPrefix().GetTarget(); t != `` {
		rec.Target = t
	}

	for _, u := range n.GetUpdate() {
		if u == nil {
			continue
		}
		val, err := decodeTypedValue(u.GetVal())
		if err != nil {
			// A notification carries many leaves sampled at one instant.
			// Failing the whole thing over one bad value from a buggy agent
			// would throw away every good counter beside it, so record the
			// leaf that failed and keep the rest.  This rides in the entry
			// rather than the log on purpose: at telemetry rates a per-sample
			// log line is its own outage.
			rec.Errors = append(rec.Errors,
				fmt.Sprintf("%s: %v", formatElems(u.GetPath().GetElem()), err))
			continue
		}
		if rec.Values == nil {
			rec.Values = map[string]any{}
		}
		// Keys are relative to the prefix so that a leaf directly under it
		// reads as "in-octets" and `json values.in-octets` just works.
		key := formatElems(u.GetPath().GetElem())
		if key == `` {
			// The update covers the prefix itself, which is the usual shape
			// for a JSON_IETF subtree. Lift its fields rather than burying
			// the whole subtree under a synthetic key.
			if m, ok := val.(map[string]any); ok {
				maps.Copy(rec.Values, m)
				continue
			}
			key = `value`
		}
		rec.Values[key] = val
	}

	// Delete paths are relative to the notification prefix exactly as update
	// paths are, so render them the same way.  Emitting them absolute would
	// have a consumer join prefix and delete into a doubled separator.
	for _, d := range n.GetDelete() {
		rec.Deletes = append(rec.Deletes, formatElems(d.GetElem()))
	}

	if len(rec.Values) == 0 && len(rec.Deletes) == 0 && len(rec.Errors) == 0 {
		return nil, nil
	}
	return json.Marshal(rec)
}

// decodeTypedValue turns a gNMI TypedValue into something json.Marshal will
// render sensibly.
func decodeTypedValue(tv *gnmipb.TypedValue) (any, error) {
	if tv == nil {
		return nil, nil
	}
	switch tv.GetValue().(type) {
	case *gnmipb.TypedValue_JsonVal:
		// value.ToScalar wraps these in a DeprecatedScalar struct, which
		// would show up in the entry as a nested {"Message":...,"Value":...},
		// so decode them ourselves.
		return decodeJSON(tv.GetJsonVal())
	case *gnmipb.TypedValue_JsonIetfVal:
		return decodeJSON(tv.GetJsonIetfVal())
	case *gnmipb.TypedValue_AsciiVal:
		return tv.GetAsciiVal(), nil
	case *gnmipb.TypedValue_ProtoBytes:
		return tv.GetProtoBytes(), nil
	case *gnmipb.TypedValue_AnyVal:
		if a := tv.GetAnyVal(); a != nil {
			return a.String(), nil
		}
		return nil, nil
	}
	return value.ToScalar(tv)
}

func decodeJSON(b []byte) (any, error) {
	if len(b) == 0 {
		return nil, nil
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(b))
	// Interface counters are uint64 and routinely exceed 2^53, where a
	// float64 would start quietly rounding them.
	d.UseNumber()
	if err := d.Decode(&v); err != nil {
		return nil, fmt.Errorf("invalid JSON value: %w", err)
	}
	return v, nil
}
