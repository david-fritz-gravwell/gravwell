/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

const (
	testPassword   = `devicePasswordMarker`
	testIngestPass = `ingestSecretMarker`

	baseGlobal = `
[Global]
Ingest-Secret = ` + testIngestPass + `
Cleartext-Backend-Target=127.0.0.1:4023
Log-Level=INFO
`

	secretConfig = baseGlobal + `
[Target "core-sw1"]
	Tag-Name=gnmi
	Host=10.0.0.5:57400
	Username=myuser
	Password=` + testPassword + `
	Subscribe-Path=/interfaces/interface/state/counters
`
)

// writeConfig drops a config into a temp dir and loads it.
func writeConfig(t *testing.T, body string) (*cfgType, error) {
	t.Helper()
	p := filepath.Join(t.TempDir(), `gnmi.conf`)
	if err := os.WriteFile(p, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return GetConfig(p, ``)
}

func mustConfig(t *testing.T, body string) *cfgType {
	t.Helper()
	cfg, err := writeConfig(t, body)
	if err != nil {
		t.Fatalf("config rejected: %v", err)
	}
	return cfg
}

// TestConfigSecretsNotReported makes sure device credentials load from the
// config but stay out of the JSON handed to SetRawConfiguration.
func TestConfigSecretsNotReported(t *testing.T) {
	cfg := mustConfig(t, secretConfig)

	tgt, ok := cfg.Target[`core-sw1`]
	if !ok {
		t.Fatal("missing core-sw1 target")
	}
	if tgt.Password != testPassword {
		t.Fatalf("bad password: %q", tgt.Password)
	}
	if tgt.Username != `myuser` {
		t.Fatalf("bad username: %q", tgt.Username)
	}

	// marshal the same way SetRawConfiguration does
	bts, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	s := string(bts)
	for _, marker := range []string{testPassword, testIngestPass} {
		if strings.Contains(s, marker) {
			t.Fatalf("reported config contains secret %q: %s", marker, s)
		}
	}
	for _, want := range []string{`gnmi`, `10.0.0.5:57400`, `myuser`} {
		if !strings.Contains(s, want) {
			t.Fatalf("reported config is missing %q: %s", want, s)
		}
	}
}

// TestUseTLSDefault covers the three-state Use-TLS pointer. Absent must mean
// true, because credentials ride in per-RPC metadata and defaulting to
// cleartext would quietly leak them.
func TestUseTLSDefault(t *testing.T) {
	tests := []struct {
		name string
		line string
		want bool
	}{
		{`absent`, ``, true},
		{`explicit true`, "\tUse-TLS=true", true},
		{`explicit false`, "\tUse-TLS=false", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := mustConfig(t, baseGlobal+`
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
`+tt.line+"\n")
			if got := cfg.Target[`t`].useTLS(); got != tt.want {
				t.Errorf("useTLS() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDefaultPort(t *testing.T) {
	tests := []struct {
		host string
		want string
	}{
		{`10.0.0.5`, `10.0.0.5:` + defaultGNMIPort},
		{`10.0.0.5:57400`, `10.0.0.5:57400`},
		{`10.0.0.5:9339`, `10.0.0.5:9339`},
		{`switch.example.com`, `switch.example.com:` + defaultGNMIPort},
		{`switch.example.com:1234`, `switch.example.com:1234`},
		{`2001:db8::1`, `[2001:db8::1]:` + defaultGNMIPort},
		{`[2001:db8::1]:57400`, `[2001:db8::1]:57400`},
		// already bracketed but portless, do not bracket it twice
		{`[2001:db8::1]`, `[2001:db8::1]:` + defaultGNMIPort},
		{`[::1]`, `[::1]:` + defaultGNMIPort},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			tgt := &target{Host: tt.host}
			got, err := tgt.dialTarget()
			if err != nil {
				t.Fatalf("dialTarget(%q) failed: %v", tt.host, err)
			}
			if got != tt.want {
				t.Errorf("dialTarget(%q) = %q, want %q", tt.host, got, tt.want)
			}
		})
	}
}

func TestConfigEnumerations(t *testing.T) {
	cfg := mustConfig(t, baseGlobal+`
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Mode=once
	Stream-Mode=on-change
	Encoding=json_ietf
`)
	tgt := cfg.Target[`t`]
	if got := tgt.listMode(); got != gnmipb.SubscriptionList_ONCE {
		t.Errorf("listMode = %v, want ONCE", got)
	}
	if got := tgt.streamMode(); got != gnmipb.SubscriptionMode_ON_CHANGE {
		t.Errorf("streamMode = %v, want ON_CHANGE", got)
	}
	if got := tgt.encoding(); got != gnmipb.Encoding_JSON_IETF {
		t.Errorf("encoding = %v, want JSON_IETF", got)
	}

	// Spelling variants all land on the same value.
	for _, v := range []string{`on-change`, `ON_CHANGE`, `onchange`, `On-Change`} {
		tgt := &target{Stream_Mode: v}
		if got := tgt.streamMode(); got != gnmipb.SubscriptionMode_ON_CHANGE {
			t.Errorf("Stream-Mode %q = %v, want ON_CHANGE", v, got)
		}
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := mustConfig(t, baseGlobal+`
[Target "t"]
	Host=10.0.0.5
	Subscribe-Path=/a/b
`)
	tgt := cfg.Target[`t`]
	if got := tgt.listMode(); got != gnmipb.SubscriptionList_STREAM {
		t.Errorf("default listMode = %v, want STREAM", got)
	}
	if got := tgt.streamMode(); got != gnmipb.SubscriptionMode_SAMPLE {
		t.Errorf("default streamMode = %v, want SAMPLE", got)
	}
	if got := tgt.encoding(); got != gnmipb.Encoding_JSON_IETF {
		t.Errorf("default encoding = %v, want JSON_IETF", got)
	}
	if got := tgt.sampleInterval(); got != defaultSampleInterval {
		t.Errorf("default sampleInterval = %v, want %v", got, defaultSampleInterval)
	}
	if got := tgt.heartbeatInterval(); got != 0 {
		t.Errorf("default heartbeatInterval = %v, want 0", got)
	}
	// An omitted Tag-Name falls back to the default tag, as elsewhere.
	if tgt.Tag_Name == `` {
		t.Error("Tag-Name was left empty rather than defaulted")
	}
}

func TestConfigIntervals(t *testing.T) {
	cfg := mustConfig(t, baseGlobal+`
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Sample-Interval=30s
	Heartbeat-Interval=5m
`)
	tgt := cfg.Target[`t`]
	if got := tgt.sampleInterval(); got != 30*time.Second {
		t.Errorf("sampleInterval = %v, want 30s", got)
	}
	if got := tgt.heartbeatInterval(); got != 5*time.Minute {
		t.Errorf("heartbeatInterval = %v, want 5m", got)
	}
}

func TestConfigOriginApplied(t *testing.T) {
	// A target level Origin fills in for paths that did not carry one, and
	// never overrides a path that did.
	cfg := mustConfig(t, baseGlobal+`
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Origin=openconfig
	Subscribe-Path=/a/b
	Subscribe-Path=eos_native:/c/d
`)
	paths, err := cfg.Target[`t`].subscriptionPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) != 2 {
		t.Fatalf("got %d paths, want 2", len(paths))
	}
	if got := paths[0].GetOrigin(); got != `openconfig` {
		t.Errorf("path 0 origin = %q, want openconfig", got)
	}
	if got := paths[1].GetOrigin(); got != `eos_native` {
		t.Errorf("path 1 origin = %q, want eos_native (should not be overridden)", got)
	}
}

func TestConfigTags(t *testing.T) {
	cfg := mustConfig(t, baseGlobal+`
[Target "a"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b

[Target "b"]
	Tag-Name=other
	Host=10.0.0.6
	Subscribe-Path=/a/b

[Target "c"]
	Tag-Name=gnmi
	Host=10.0.0.7
	Subscribe-Path=/a/b
`)
	tags, err := cfg.Tags()
	if err != nil {
		t.Fatal(err)
	}
	// deduplicated and sorted
	if len(tags) != 2 || tags[0] != `gnmi` || tags[1] != `other` {
		t.Errorf("tags = %v, want [gnmi other]", tags)
	}
}

func TestConfigRejections(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{`no targets`, baseGlobal},
		{`missing host`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Subscribe-Path=/a/b
`},
		{`no subscribe path`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
`},
		{`bad subscribe path`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=no-leading-slash
`},
		{`bad mode`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Mode=nonsense
`},
		{`bad stream mode`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Stream-Mode=nonsense
`},
		{`bad encoding`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Encoding=nonsense
`},
		{`bad sample interval`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Sample-Interval=soon
`},
		{`negative sample interval`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Sample-Interval=-10s
`},
		{`bad heartbeat interval`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Heartbeat-Interval=often
`},
		{`bad source override`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Source-Override=not-an-ip
`},
		{`bad tag name`, baseGlobal + `
[Target "t"]
	Tag-Name=has a space
	Host=10.0.0.5
	Subscribe-Path=/a/b
`},
		// TLS settings that cannot take effect almost always mean somebody
		// expected encryption they are not getting.
		{`ca cert without tls`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Use-TLS=false
	CA-Cert=/tmp/some-ca.pem
`},
		{`skip verify without tls`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Use-TLS=false
	Insecure-Skip-TLS-Verify=true
`},
		{`server name without tls`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	Use-TLS=false
	Server-Name=sw1.example.com
`},
		{`missing ca cert file`, baseGlobal + `
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
	CA-Cert=/nonexistent/path/to/ca.pem
`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if cfg, err := writeConfig(t, tt.body); err == nil {
				t.Errorf("config should have been rejected, got %#v", cfg)
			}
		})
	}
}

// TestKeepaliveDefaults makes sure the keepalive interval stays clear of the
// 5 minute floor grpc-go servers enforce, that it is overridable, and that an
// explicit 0 turns keepalive off rather than falling back to the default.
// TestSkipVerifyVoidsCACert makes sure a pinned CA cannot be silently
// defeated.  InsecureSkipVerify turns chain verification off entirely, so
// RootCAs is never consulted; accepting both would leave an operator believing
// they pinned a private CA while the ingester takes any certificate at all and
// hands the device password to whatever answered.
func TestSkipVerifyVoidsCACert(t *testing.T) {
	dir := t.TempDir()
	ca := filepath.Join(dir, `ca.pem`)
	if err := os.WriteFile(ca, testCAPEM(t), 0600); err != nil {
		t.Fatal(err)
	}

	both := &target{
		Tag_Name:                 `gnmi`,
		Host:                     `10.0.0.5`,
		Subscribe_Path:           []string{`/a/b`},
		CA_Cert:                  ca,
		Insecure_Skip_TLS_Verify: true,
	}
	if err := both.validate(); err == nil {
		t.Error("Insecure-Skip-TLS-Verify together with CA-Cert was accepted")
	}

	// Each on its own is still legitimate: skip-verify for a self signed
	// device cert, a pinned CA for a private PKI.
	skip := &target{
		Tag_Name:                 `gnmi`,
		Host:                     `10.0.0.5`,
		Subscribe_Path:           []string{`/a/b`},
		Insecure_Skip_TLS_Verify: true,
	}
	if err := skip.validate(); err != nil {
		t.Errorf("Insecure-Skip-TLS-Verify alone was rejected: %v", err)
	}
	pin := &target{
		Tag_Name:       `gnmi`,
		Host:           `10.0.0.5`,
		Subscribe_Path: []string{`/a/b`},
		CA_Cert:        ca,
	}
	if err := pin.validate(); err != nil {
		t.Errorf("CA-Cert alone was rejected: %v", err)
	}
}

// TestHostRejections makes sure a malformed Host is caught at config load
// rather than validating clean and failing forever at dial.
func TestHostRejections(t *testing.T) {
	bad := []string{
		`http://10.0.0.5:57400`,
		`10.0.0.5 57400`,
		`!!!!`,
		`10.0.0.5:`,
		`10.0.0.5:0`,
		`10.0.0.5:notaport`,
		`10.0.0.5:99999`,
		`sw..example.com`,
		`sw/example.com`,
	}
	for _, h := range bad {
		t.Run(h, func(t *testing.T) {
			if _, err := (&target{Host: h}).dialTarget(); err == nil {
				t.Errorf("Host %q was accepted", h)
			}
		})
	}
	// Things that must keep working.
	for _, h := range []string{`10.0.0.5`, `switch.example.com`, `switch.example.com.`,
		`sw-1_a.example.com:9339`, `2001:db8::1`, `[2001:db8::1]:57400`} {
		t.Run(h, func(t *testing.T) {
			if _, err := (&target{Host: h}).dialTarget(); err != nil {
				t.Errorf("Host %q was rejected: %v", h, err)
			}
		})
	}
}

func TestKeepaliveDefaults(t *testing.T) {
	base := &target{}
	if got := base.keepaliveInterval(); got != defaultKeepaliveInterval {
		t.Errorf("default keepalive interval = %v, want %v", got, defaultKeepaliveInterval)
	}
	if defaultKeepaliveInterval <= 5*time.Minute {
		t.Errorf("default keepalive interval %v is at or under the 5m server floor",
			defaultKeepaliveInterval)
	}
	if got := base.keepaliveTimeout(); got != defaultKeepaliveTimeout {
		t.Errorf("default keepalive timeout = %v, want %v", got, defaultKeepaliveTimeout)
	}

	set := &target{Keepalive_Interval: `45s`, Keepalive_Timeout: `5s`}
	if got := set.keepaliveInterval(); got != 45*time.Second {
		t.Errorf("keepalive interval = %v, want 45s", got)
	}
	if got := set.keepaliveTimeout(); got != 5*time.Second {
		t.Errorf("keepalive timeout = %v, want 5s", got)
	}

	if got := (&target{Keepalive_Interval: `0`}).keepaliveInterval(); got != 0 {
		t.Errorf("explicit 0 keepalive interval = %v, want 0 (disabled)", got)
	}
}

// TestMaxMessageSize makes sure the gRPC receive cap is raised well past the
// 4MB library default and that out of range values fall back rather than
// producing a nonsensical limit.
func TestMaxMessageSize(t *testing.T) {
	const grpcDefault = 4 * 1024 * 1024
	if got := (&target{}).maxMessageSize(); got <= grpcDefault {
		t.Errorf("default max message size = %d, want more than the %d grpc default",
			got, grpcDefault)
	}
	if got := (&target{Max_Message_Size: 64}).maxMessageSize(); got != 64*1024*1024 {
		t.Errorf("max message size = %d, want %d", got, 64*1024*1024)
	}
	for _, mb := range []int{0, -1, maxMaxMessageSizeMB + 1} {
		if got := (&target{Max_Message_Size: mb}).maxMessageSize(); got != defaultMaxMessageSizeMB*1024*1024 {
			t.Errorf("max message size for %d MB = %d, want the default", mb, got)
		}
	}
}

// TestKeepaliveAndSizeRejections makes sure junk in the new knobs is caught at
// config load rather than at dial time.
func TestKeepaliveAndSizeRejections(t *testing.T) {
	bad := []string{
		"\tKeepalive-Interval=soon",
		"\tKeepalive-Timeout=-5s",
		"\tMax-Message-Size=-1",
		"\tMax-Message-Size=99999",
	}
	for _, line := range bad {
		t.Run(strings.TrimSpace(line), func(t *testing.T) {
			_, err := writeConfig(t, baseGlobal+`
[Target "t"]
	Tag-Name=gnmi
	Host=10.0.0.5
	Subscribe-Path=/a/b
`+line+"\n")
			if err == nil {
				t.Errorf("%q was accepted", strings.TrimSpace(line))
			}
		})
	}
}

func TestCACertValidation(t *testing.T) {
	dir := t.TempDir()

	// A file that is not PEM at all must be rejected rather than silently
	// producing an empty trust pool that fails every handshake later.
	junk := filepath.Join(dir, `junk.pem`)
	if err := os.WriteFile(junk, []byte("this is not a certificate\n"), 0600); err != nil {
		t.Fatal(err)
	}
	tgt := &target{Host: `10.0.0.5`, CA_Cert: junk}
	if _, err := tgt.tlsConfig(); err == nil {
		t.Error("a CA-Cert with no usable PEM should be rejected")
	}

	// A real certificate loads into the pool.
	good := filepath.Join(dir, `ca.pem`)
	if err := os.WriteFile(good, testCAPEM(t), 0600); err != nil {
		t.Fatal(err)
	}
	tgt = &target{Host: `10.0.0.5`, CA_Cert: good}
	tcfg, err := tgt.tlsConfig()
	if err != nil {
		t.Fatalf("valid CA-Cert rejected: %v", err)
	}
	if tcfg.RootCAs == nil {
		t.Error("RootCAs was not populated")
	}
	// With no explicit Server-Name the device host is used for verification.
	if tcfg.ServerName != `10.0.0.5` {
		t.Errorf("ServerName = %q, want 10.0.0.5", tcfg.ServerName)
	}

	// An explicit Server-Name wins.
	tgt = &target{Host: `10.0.0.5`, Server_Name: `sw1.example.com`}
	if tcfg, err = tgt.tlsConfig(); err != nil {
		t.Fatal(err)
	}
	if tcfg.ServerName != `sw1.example.com` {
		t.Errorf("ServerName = %q, want sw1.example.com", tcfg.ServerName)
	}
}

// testCAPEM mints a throwaway self signed CA, used only to prove that PEM
// parsing and the trust pool are wired up.
func testCAPEM(t *testing.T) []byte {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: `gnmi-test-ca`},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: `CERTIFICATE`, Bytes: der})
}
