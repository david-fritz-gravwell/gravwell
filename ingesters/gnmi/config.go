/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"

	"github.com/gravwell/gravwell/v3/ingest"
	"github.com/gravwell/gravwell/v3/ingest/attach"
	"github.com/gravwell/gravwell/v3/ingest/config"
	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/gravwell/gravwell/v3/ingest/processors"
)

const (
	defaultSampleInterval = 10 * time.Second
	defaultGNMIPort       = `57400`

	// grpc-go servers refuse keepalive pings faster than one every 5 minutes
	// by default and hang up after three violations, and the strike counter
	// only clears when the server sends data.  A quiet ON_CHANGE stream is
	// the case that gets killed, so stay clear of that floor by default.
	defaultKeepaliveInterval = 6 * time.Minute
	defaultKeepaliveTimeout  = 20 * time.Second

	// gRPC defaults to a 4MB receive cap, which a ONCE dump or a wide
	// JSON_IETF subtree will exceed on a large chassis.
	defaultMaxMessageSizeMB = 16
	maxMaxMessageSizeMB     = 2048
)

type target struct {
	Tag_Name string
	Host     string
	Username string
	Password string `json:"-"` // DO NOT send this when marshalling

	// Use_TLS is a pointer so that we can tell "absent" from an explicit
	// false; absent means true. gcfg allocates pointer fields on assignment.
	Use_TLS                  *bool
	Insecure_Skip_TLS_Verify bool
	CA_Cert                  string
	Server_Name              string

	Subscribe_Path     []string
	Mode               string
	Stream_Mode        string
	Sample_Interval    string
	Heartbeat_Interval string
	Suppress_Redundant bool
	Updates_Only       bool
	Origin             string
	Encoding           string

	Keepalive_Interval string
	Keepalive_Timeout  string
	Max_Message_Size   int // megabytes

	Source_Override string
	Preprocessor    []string
}

type global struct {
	config.IngestConfig
}

type cfgReadType struct {
	Global       global
	Attach       attach.AttachConfig
	Target       map[string]*target
	Preprocessor processors.ProcessorConfig
}

type cfgType struct {
	config.IngestConfig
	Attach       attach.AttachConfig
	Target       map[string]*target
	Preprocessor processors.ProcessorConfig
}

var subscriptionListModes = map[string]gnmipb.SubscriptionList_Mode{
	`STREAM`: gnmipb.SubscriptionList_STREAM,
	`ONCE`:   gnmipb.SubscriptionList_ONCE,
	`POLL`:   gnmipb.SubscriptionList_POLL,
}

var streamModes = map[string]gnmipb.SubscriptionMode{
	`TARGETDEFINED`: gnmipb.SubscriptionMode_TARGET_DEFINED,
	`ONCHANGE`:      gnmipb.SubscriptionMode_ON_CHANGE,
	`SAMPLE`:        gnmipb.SubscriptionMode_SAMPLE,
}

var encodings = map[string]gnmipb.Encoding{
	`JSONIETF`: gnmipb.Encoding_JSON_IETF,
	`JSON`:     gnmipb.Encoding_JSON,
	`PROTO`:    gnmipb.Encoding_PROTO,
	`BYTES`:    gnmipb.Encoding_BYTES,
	`ASCII`:    gnmipb.Encoding_ASCII,
}

// normName normalizes a config enumeration so that the obvious spellings all
// land on the same value: "on-change", "ON_CHANGE" and "onchange" are one.
func normName(v string) string {
	v = strings.ToUpper(strings.TrimSpace(v))
	v = strings.ReplaceAll(v, `-`, ``)
	v = strings.ReplaceAll(v, `_`, ``)
	return v
}

// nameList returns the sorted set of names accepted by an enumeration map, it
// is only used to build error messages.
func nameList[T comparable](mp map[string]T) string {
	names := make([]string, 0, len(mp))
	for k := range mp {
		names = append(names, k)
	}
	sort.Strings(names)
	return strings.Join(names, `, `)
}

func GetConfig(path, overlayPath string) (*cfgType, error) {
	//read into the intermediary type to maintain backwards compatibility with the old system
	var cr cfgReadType
	if err := config.LoadConfigFile(&cr, path); err != nil {
		return nil, err
	} else if err = config.LoadConfigOverlays(&cr, overlayPath); err != nil {
		return nil, err
	}
	c := &cfgType{
		IngestConfig: cr.Global.IngestConfig,
		Attach:       cr.Attach,
		Target:       cr.Target,
		Preprocessor: cr.Preprocessor,
	}

	if err := c.Verify(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *cfgType) Verify() error {
	//verify the global parameters
	if err := c.IngestConfig.Verify(); err != nil {
		return err
	} else if err = c.Attach.Verify(); err != nil {
		return err
	}

	if len(c.Target) == 0 {
		return errors.New("No targets specified")
	}

	if err := c.Preprocessor.Validate(); err != nil {
		return err
	}

	for k, v := range c.Target {
		if err := v.validate(); err != nil {
			return fmt.Errorf("Target %s is invalid: %v", k, err)
		}
		if err := c.Preprocessor.CheckProcessors(v.Preprocessor); err != nil {
			return fmt.Errorf("Target %s preprocessor invalid: %v", k, err)
		}
	}

	return nil
}

func (t *target) validate() error {
	if len(t.Tag_Name) == 0 {
		t.Tag_Name = entry.DefaultTagName
	}
	if ingest.CheckTag(t.Tag_Name) != nil {
		return fmt.Errorf("Invalid characters in the Tag-Name %q", t.Tag_Name)
	}

	if strings.TrimSpace(t.Host) == `` {
		return errors.New("Host is required")
	} else if _, err := t.dialTarget(); err != nil {
		return err
	}

	if t.Source_Override != `` && net.ParseIP(t.Source_Override) == nil {
		return fmt.Errorf("Source-Override %s is not a valid IP address", t.Source_Override)
	}

	if len(t.Subscribe_Path) == 0 {
		return errors.New("at least one Subscribe-Path is required")
	}
	if _, err := t.subscriptionPaths(); err != nil {
		return err
	}

	if v := normName(t.Mode); v != `` {
		if _, ok := subscriptionListModes[v]; !ok {
			return fmt.Errorf("Invalid Mode %v. Supported modes: %s", t.Mode, nameList(subscriptionListModes))
		}
	}
	if v := normName(t.Stream_Mode); v != `` {
		if _, ok := streamModes[v]; !ok {
			return fmt.Errorf("Invalid Stream-Mode %v. Supported modes: %s", t.Stream_Mode, nameList(streamModes))
		}
	}
	if v := normName(t.Encoding); v != `` {
		if _, ok := encodings[v]; !ok {
			return fmt.Errorf("Invalid Encoding %v. Supported encodings: %s", t.Encoding, nameList(encodings))
		}
	}

	if _, err := parseInterval(t.Sample_Interval); err != nil {
		return fmt.Errorf("Invalid Sample-Interval: %v", err)
	}
	if _, err := parseInterval(t.Heartbeat_Interval); err != nil {
		return fmt.Errorf("Invalid Heartbeat-Interval: %v", err)
	}
	if _, err := parseInterval(t.Keepalive_Interval); err != nil {
		return fmt.Errorf("Invalid Keepalive-Interval: %v", err)
	}
	if _, err := parseInterval(t.Keepalive_Timeout); err != nil {
		return fmt.Errorf("Invalid Keepalive-Timeout: %v", err)
	}
	if t.Max_Message_Size < 0 || t.Max_Message_Size > maxMaxMessageSizeMB {
		return fmt.Errorf("Max-Message-Size %d is out of range, 0 (default) to %d MB",
			t.Max_Message_Size, maxMaxMessageSizeMB)
	}

	// Catch TLS settings that cannot do anything, they are almost always a
	// sign that somebody expected the connection to be encrypted when it
	// is not, and the credentials go out in the clear if we let it slide.
	if !t.useTLS() {
		if t.CA_Cert != `` {
			return errors.New("CA-Cert is set but Use-TLS is false")
		} else if t.Insecure_Skip_TLS_Verify {
			return errors.New("Insecure-Skip-TLS-Verify is set but Use-TLS is false")
		} else if t.Server_Name != `` {
			return errors.New("Server-Name is set but Use-TLS is false")
		}
	} else {
		// Insecure-Skip-TLS-Verify turns off chain verification altogether,
		// which means a pinned CA-Cert is never consulted.  Accepting both
		// would let an operator believe they pinned a private CA while the
		// ingester takes any certificate at all and hands the device password
		// to whatever answered.
		if t.Insecure_Skip_TLS_Verify && t.CA_Cert != `` {
			return errors.New("Insecure-Skip-TLS-Verify cannot be combined with CA-Cert, skipping verification ignores the pinned CA")
		}
		if _, err := t.tlsConfig(); err != nil {
			return err
		}
	}

	return nil
}

// useTLS reports whether the connection should be encrypted. An unset Use-TLS
// means true: credentials ride in per-RPC metadata, so defaulting to cleartext
// would quietly leak them.
func (t *target) useTLS() bool {
	return t.Use_TLS == nil || *t.Use_TLS
}

// dialTarget returns the host:port to dial, defaulting the port when the
// config only gives a host.
//
// The host and port are checked for real rather than round tripped through
// net.JoinHostPort, which will happily bracket arbitrary junk and hand back
// something SplitHostPort accepts.  Without this a pasted URL or a stray space
// validates clean and the target instead fails forever at dial with an opaque
// resolver error.
func (t *target) dialTarget() (string, error) {
	h := strings.TrimSpace(t.Host)
	host, port, err := net.SplitHostPort(h)
	if err != nil {
		// No port given, or something malformed.  A bare IPv6 literal may
		// already be bracketed, and JoinHostPort would bracket it a second
		// time, so peel those off first.
		host, port = h, defaultGNMIPort
		if len(host) > 1 && host[0] == '[' && host[len(host)-1] == ']' {
			host = host[1 : len(host)-1]
		}
	}
	if err := validateHost(host); err != nil {
		return ``, fmt.Errorf("Host %q is not a valid host or host:port: %v", t.Host, err)
	}
	pn, err := strconv.ParseUint(port, 10, 16)
	if err != nil || pn == 0 {
		return ``, fmt.Errorf("Host %q has an invalid port %q", t.Host, port)
	}
	return net.JoinHostPort(host, port), nil
}

// validateHost checks that a host is an IP literal or something that could
// plausibly resolve, so that junk is caught at config load.
func validateHost(h string) error {
	if h == `` {
		return errors.New("empty host")
	}
	if net.ParseIP(h) != nil {
		return nil
	}
	if len(h) > 253 {
		return errors.New("host name is too long")
	}
	// A fully qualified name may carry a single trailing dot.
	for _, label := range strings.Split(strings.TrimSuffix(h, `.`), `.`) {
		if label == `` {
			return errors.New("host name has an empty label")
		}
		for i := 0; i < len(label); i++ {
			switch c := label[i]; {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
			case c >= '0' && c <= '9', c == '-', c == '_':
			default:
				return fmt.Errorf("invalid character %q in host name", string(c))
			}
		}
	}
	return nil
}

// tlsConfig builds the client TLS configuration, reading the pinned CA off
// disk if one was given.
func (t *target) tlsConfig() (*tls.Config, error) {
	cfg := &tls.Config{
		InsecureSkipVerify: t.Insecure_Skip_TLS_Verify,
		ServerName:         t.Server_Name,
	}
	if cfg.ServerName == `` {
		host, _, err := net.SplitHostPort(mustDialTarget(t))
		if err == nil {
			cfg.ServerName = host
		}
	}
	if t.CA_Cert != `` {
		pem, err := os.ReadFile(t.CA_Cert)
		if err != nil {
			return nil, fmt.Errorf("failed to read CA-Cert %s: %v", t.CA_Cert, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA-Cert %s contains no usable PEM certificates", t.CA_Cert)
		}
		cfg.RootCAs = pool
	}
	return cfg, nil
}

// mustDialTarget is the error swallowing form of dialTarget, for the places
// that have already validated the host.
func mustDialTarget(t *target) string {
	s, err := t.dialTarget()
	if err != nil {
		return t.Host
	}
	return s
}

// subscriptionPaths parses every Subscribe-Path, applying the target's Origin
// to any path that did not carry one of its own.
func (t *target) subscriptionPaths() ([]*gnmipb.Path, error) {
	paths := make([]*gnmipb.Path, 0, len(t.Subscribe_Path))
	for _, raw := range t.Subscribe_Path {
		p, err := parsePath(raw)
		if err != nil {
			return nil, fmt.Errorf("Invalid Subscribe-Path: %v", err)
		}
		if p.GetOrigin() == `` && t.Origin != `` {
			p.Origin = t.Origin
		}
		paths = append(paths, p)
	}
	return paths, nil
}

func (t *target) listMode() gnmipb.SubscriptionList_Mode {
	if m, ok := subscriptionListModes[normName(t.Mode)]; ok {
		return m
	}
	return gnmipb.SubscriptionList_STREAM
}

func (t *target) streamMode() gnmipb.SubscriptionMode {
	if m, ok := streamModes[normName(t.Stream_Mode)]; ok {
		return m
	}
	return gnmipb.SubscriptionMode_SAMPLE
}

func (t *target) encoding() gnmipb.Encoding {
	if e, ok := encodings[normName(t.Encoding)]; ok {
		return e
	}
	return gnmipb.Encoding_JSON_IETF
}

func (t *target) sampleInterval() time.Duration {
	if d, err := parseInterval(t.Sample_Interval); err == nil && d > 0 {
		return d
	}
	return defaultSampleInterval
}

// keepaliveInterval is how often to ping an idle connection.  An unset value
// means the default; an explicit 0 disables keepalive entirely, for devices
// that hang up on pings no matter how infrequent.
func (t *target) keepaliveInterval() time.Duration {
	if strings.TrimSpace(t.Keepalive_Interval) == `` {
		return defaultKeepaliveInterval
	}
	d, err := parseInterval(t.Keepalive_Interval)
	if err != nil {
		return defaultKeepaliveInterval
	}
	return d
}

func (t *target) keepaliveTimeout() time.Duration {
	if d, err := parseInterval(t.Keepalive_Timeout); err == nil && d > 0 {
		return d
	}
	return defaultKeepaliveTimeout
}

// maxMessageSize is the largest gRPC message to accept, in bytes.
func (t *target) maxMessageSize() int {
	mb := t.Max_Message_Size
	if mb <= 0 || mb > maxMaxMessageSizeMB {
		mb = defaultMaxMessageSizeMB
	}
	return mb * 1024 * 1024
}

func (t *target) heartbeatInterval() time.Duration {
	d, err := parseInterval(t.Heartbeat_Interval)
	if err != nil {
		return 0
	}
	return d
}

// parseInterval parses an optional duration, where empty and "0" both mean
// unset. Negative durations are rejected rather than silently clamped.
func parseInterval(v string) (time.Duration, error) {
	v = strings.TrimSpace(v)
	if v == `` {
		return 0, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, err
	} else if d < 0 {
		return 0, fmt.Errorf("%v is negative", v)
	}
	return d, nil
}

func (c *cfgType) Tags() ([]string, error) {
	var tags []string
	tagMp := make(map[string]bool, 1)

	for _, v := range c.Target {
		if len(v.Tag_Name) == 0 {
			continue
		}
		if _, ok := tagMp[v.Tag_Name]; !ok {
			tags = append(tags, v.Tag_Name)
			tagMp[v.Tag_Name] = true
		}
	}

	if len(tags) == 0 {
		return nil, errors.New("No tags specified")
	}
	sort.Strings(tags)
	return tags, nil
}

func (c *cfgType) IngestBaseConfig() config.IngestConfig {
	return c.IngestConfig
}

func (c *cfgType) AttachConfig() attach.AttachConfig {
	return c.Attach
}

func (g *global) Verify() (err error) {
	if err = g.IngestConfig.Verify(); err != nil {
		return
	}
	return
}
