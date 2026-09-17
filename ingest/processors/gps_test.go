//go:build linux || darwin
// +build linux darwin

/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package processors

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gravwell/gcfg"
	"github.com/gravwell/gravwell/v3/ingest/entry"
)

func TestGPSConfigDefaults(t *testing.T) {
	b := []byte(`
	[preprocessor "gps1"]
		type = gps
	`)
	tc := struct {
		Preprocessor ProcessorConfig
	}{}
	if err := gcfg.ReadStringInto(&tc, string(b)); err != nil {
		t.Fatal(err)
	}
	vc, ok := tc.Preprocessor[`gps1`]
	if !ok {
		t.Fatal("missing gps1 preprocessor")
	}
	cfg, err := GPSLoadConfig(vc)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server != defaultGPSServer {
		t.Fatalf("bad default server: %q != %q", cfg.Server, defaultGPSServer)
	}
	if cfg.maxAge != defaultGPSMaxAge {
		t.Fatalf("bad default max age: %v != %v", cfg.maxAge, defaultGPSMaxAge)
	}
	if cfg.Lat_Name != defaultGPSLatName || cfg.Lon_Name != defaultGPSLonName || cfg.Alt_Name != defaultGPSAltName {
		t.Fatalf("bad default EV names: %q %q %q", cfg.Lat_Name, cfg.Lon_Name, cfg.Alt_Name)
	}
}

func TestGPSConfigOverrides(t *testing.T) {
	b := []byte(`
	[preprocessor "gps1"]
		type = gps
		Server = gpsd.example.com
		Max-Fix-Age = 2m
		Lat-Name = Latitude
		Lon-Name = Longitude
		Alt-Name = Altitude
		Require-3D-Fix = true
		Disable-Altitude = true
	`)
	tc := struct {
		Preprocessor ProcessorConfig
	}{}
	if err := gcfg.ReadStringInto(&tc, string(b)); err != nil {
		t.Fatal(err)
	}
	cfg, err := GPSLoadConfig(tc.Preprocessor[`gps1`])
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server != `gpsd.example.com:2947` {
		t.Fatalf("default port not applied: %q", cfg.Server)
	}
	if cfg.maxAge != 2*time.Minute {
		t.Fatalf("bad max age: %v", cfg.maxAge)
	}
	if cfg.Lat_Name != `Latitude` || cfg.Lon_Name != `Longitude` || cfg.Alt_Name != `Altitude` {
		t.Fatalf("bad EV names: %q %q %q", cfg.Lat_Name, cfg.Lon_Name, cfg.Alt_Name)
	}
	if !cfg.Require_3D_Fix || !cfg.Disable_Altitude {
		t.Fatalf("bad flags: %v %v", cfg.Require_3D_Fix, cfg.Disable_Altitude)
	}
}

func TestGPSConfigErrors(t *testing.T) {
	tsts := []struct {
		name string
		body string
		err  error
	}{
		{
			name: `bad max fix age`,
			body: "[preprocessor \"gps1\"]\ntype = gps\nMax-Fix-Age = chicken\n",
			err:  ErrGPSInvalidMaxAge,
		},
		{
			name: `negative max fix age`,
			body: "[preprocessor \"gps1\"]\ntype = gps\nMax-Fix-Age = -5s\n",
			err:  ErrGPSInvalidMaxAge,
		},
		{
			name: `duplicate lat and lon names`,
			body: "[preprocessor \"gps1\"]\ntype = gps\nLat-Name = Loc\nLon-Name = Loc\n",
			err:  ErrGPSDuplicateEVName,
		},
		{
			name: `duplicate alt name`,
			body: "[preprocessor \"gps1\"]\ntype = gps\nAlt-Name = Lat\n",
			err:  ErrGPSDuplicateEVName,
		},
	}
	for _, tst := range tsts {
		t.Run(tst.name, func(t *testing.T) {
			tc := struct {
				Preprocessor ProcessorConfig
			}{}
			if err := gcfg.ReadStringInto(&tc, tst.body); err != nil {
				t.Fatal(err)
			}
			if _, err := GPSLoadConfig(tc.Preprocessor[`gps1`]); err != tst.err {
				t.Fatalf("expected %v, got %v", tst.err, err)
			}
		})
	}
}

// a duplicate alt name is fine when altitude is turned off entirely
func TestGPSConfigDuplicateAltNameAllowedWhenDisabled(t *testing.T) {
	tc := struct {
		Preprocessor ProcessorConfig
	}{}
	body := "[preprocessor \"gps1\"]\ntype = gps\nAlt-Name = Lat\nDisable-Altitude = true\n"
	if err := gcfg.ReadStringInto(&tc, body); err != nil {
		t.Fatal(err)
	}
	if _, err := GPSLoadConfig(tc.Preprocessor[`gps1`]); err != nil {
		t.Fatal(err)
	}
}

func TestGPSCheckProcessor(t *testing.T) {
	if err := CheckProcessor(GPSProcessor); err != nil {
		t.Fatal(err)
	}
}

func TestNormalizeGPSServer(t *testing.T) {
	tsts := []struct {
		in  string
		out string
		err bool
	}{
		{in: `127.0.0.1:2947`, out: `127.0.0.1:2947`},
		{in: `127.0.0.1`, out: `127.0.0.1:2947`},
		{in: `gpsd.example.com`, out: `gpsd.example.com:2947`},
		{in: `gpsd.example.com:1234`, out: `gpsd.example.com:1234`},
		{in: `[::1]:2947`, out: `[::1]:2947`},
	}
	for _, tst := range tsts {
		got, err := normalizeGPSServer(tst.in)
		if tst.err {
			if err == nil {
				t.Fatalf("%q: expected error", tst.in)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", tst.in, err)
		}
		if got != tst.out {
			t.Fatalf("%q: %q != %q", tst.in, got, tst.out)
		}
	}
}

// fakeGPSD is a minimal stand in for gpsd: it accepts a connection, waits for
// the WATCH command, then streams whatever reports the test hands it.
type fakeGPSD struct {
	ln      net.Listener
	mtx     sync.Mutex
	conns   []net.Conn
	reports chan string
	wg      sync.WaitGroup
}

func newFakeGPSD(t *testing.T) *fakeGPSD {
	t.Helper()
	ln, err := net.Listen(`tcp`, `127.0.0.1:0`)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeGPSD{
		ln:      ln,
		reports: make(chan string, 16),
	}
	f.wg.Add(1)
	go f.accept()
	return f
}

func (f *fakeGPSD) accept() {
	defer f.wg.Done()
	for {
		conn, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.mtx.Lock()
		f.conns = append(f.conns, conn)
		f.mtx.Unlock()
		f.wg.Add(1)
		go f.serve(conn)
	}
}

func (f *fakeGPSD) serve(conn net.Conn) {
	defer f.wg.Done()
	defer conn.Close()
	fmt.Fprintf(conn, "{\"class\":\"VERSION\",\"release\":\"3.22\"}\n")
	// wait for the WATCH command before streaming anything
	br := bufio.NewReader(conn)
	line, err := br.ReadString('\n')
	if err != nil || !strings.HasPrefix(line, `?WATCH=`) {
		return
	}
	fmt.Fprintf(conn, "{\"class\":\"DEVICES\",\"devices\":[]}\n")
	for rpt := range f.reports {
		if _, err := fmt.Fprintf(conn, "%s\n", rpt); err != nil {
			return
		}
	}
}

func (f *fakeGPSD) addr() string { return f.ln.Addr().String() }

func (f *fakeGPSD) send(rpt string) { f.reports <- rpt }

func (f *fakeGPSD) Close() {
	f.ln.Close()
	close(f.reports)
	f.mtx.Lock()
	for _, c := range f.conns {
		c.Close()
	}
	f.mtx.Unlock()
	f.wg.Wait()
}

// waitForFix polls until the processor has a usable fix, so tests do not race
// the background gpsd reader.
func waitForFix(t *testing.T, g *GPS) gpsFix {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fix, _, _, _, _, ok := g.currentFix(); ok {
			return fix
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for a fix")
	return gpsFix{}
}

func waitForNoFix(t *testing.T, g *GPS) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, _, _, _, ok := g.currentFix(); !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the fix to clear")
}

func newTestGPS(t *testing.T, f *fakeGPSD, mod func(*GPSConfig)) *GPS {
	t.Helper()
	cfg := GPSConfig{Server: f.addr()}
	if mod != nil {
		mod(&cfg)
	}
	g, err := NewGPSProcessor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestGPSAttach3DFix(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","device":"/dev/ttyUSB0","mode":3,"lat":44.0682,"lon":-114.7420,"altMSL":1875.5}`)
	waitForFix(t, g)

	ents := []*entry.Entry{{Data: []byte(`testing`)}}
	ents, err := g.Process(ents)
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 {
		t.Fatalf("bad entry count %d", len(ents))
	}
	checkFloatEV(t, ents[0], defaultGPSLatName, 44.0682)
	checkFloatEV(t, ents[0], defaultGPSLonName, -114.7420)
	checkFloatEV(t, ents[0], defaultGPSAltName, 1875.5)
}

// a 2D fix has no altitude component, so only lat and lon should land
func TestGPSAttach2DFix(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","mode":2,"lat":44.0682,"lon":-114.7420}`)
	waitForFix(t, g)

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	checkFloatEV(t, ents[0], defaultGPSLatName, 44.0682)
	checkFloatEV(t, ents[0], defaultGPSLonName, -114.7420)
	if _, ok := ents[0].GetEnumeratedValue(defaultGPSAltName); ok {
		t.Fatal("2D fix should not produce an altitude EV")
	}
}

// older gpsd releases emit "alt" rather than "altMSL"
func TestGPSLegacyAltField(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"alt":100.25}`)
	waitForFix(t, g)

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	checkFloatEV(t, ents[0], defaultGPSAltName, 100.25)
}

func TestGPSDisableAltitude(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, func(c *GPSConfig) { c.Disable_Altitude = true })
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":100.25}`)
	waitForFix(t, g)

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	checkFloatEV(t, ents[0], defaultGPSLatName, 1.5)
	if _, ok := ents[0].GetEnumeratedValue(defaultGPSAltName); ok {
		t.Fatal("altitude EV attached while disabled")
	}
}

func TestGPSCustomEVNames(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, func(c *GPSConfig) {
		c.Lat_Name = `Latitude`
		c.Lon_Name = `Longitude`
		c.Alt_Name = `Altitude`
	})
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":100.25}`)
	waitForFix(t, g)

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	checkFloatEV(t, ents[0], `Latitude`, 1.5)
	checkFloatEV(t, ents[0], `Longitude`, 2.5)
	checkFloatEV(t, ents[0], `Altitude`, 100.25)
	if _, ok := ents[0].GetEnumeratedValue(defaultGPSLatName); ok {
		t.Fatal("default lat EV attached alongside the custom name")
	}
}

// with no fix at all the entries must come out exactly as they went in
func TestGPSNoFixPassthrough(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	ents := []*entry.Entry{{Data: []byte(`testing`)}, {Data: []byte(`testing2`)}}
	rset, err := g.Process(ents)
	if err != nil {
		t.Fatal(err)
	}
	if len(rset) != 2 {
		t.Fatalf("bad entry count %d", len(rset))
	}
	for _, ent := range rset {
		if ent.EVB.Populated() {
			t.Fatal("EVs attached without a fix")
		}
	}
}

// mode 1 means the receiver knows it has lost its fix, we must stop attaching
func TestGPSFixLoss(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":100.25}`)
	waitForFix(t, g)
	f.send(`{"class":"TPV","mode":1}`)
	waitForNoFix(t, g)

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	if ents[0].EVB.Populated() {
		t.Fatal("EVs attached after the fix was lost")
	}
}

func TestGPSRequire3DFix(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, func(c *GPSConfig) { c.Require_3D_Fix = true })
	defer g.Close()

	// a 2D fix must be ignored entirely
	f.send(`{"class":"TPV","mode":2,"lat":1.5,"lon":2.5}`)
	// follow it with a 3D fix so we have something deterministic to wait on
	f.send(`{"class":"TPV","mode":3,"lat":3.5,"lon":4.5,"altMSL":10}`)
	fix := waitForFix(t, g)
	if fix.lat != 3.5 || fix.lon != 4.5 {
		t.Fatalf("2D fix was accepted while requiring 3D: %v", fix)
	}
}

// a stale fix should stop being attached once it ages out
func TestGPSStaleFix(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, func(c *GPSConfig) { c.Max_Fix_Age = `50ms` })
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":10}`)
	waitForFix(t, g)
	waitForNoFix(t, g) // the 50ms window closes on its own

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	if ents[0].EVB.Populated() {
		t.Fatal("EVs attached from a stale fix")
	}
}

// Max-Fix-Age of 0 disables the staleness check entirely
func TestGPSStalenessDisabled(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, func(c *GPSConfig) { c.Max_Fix_Age = `0` })
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":10}`)
	waitForFix(t, g)

	// backdate the fix well past any sane max age
	g.mtx.Lock()
	g.fix.rx = time.Now().Add(-24 * time.Hour)
	g.mtx.Unlock()

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	checkFloatEV(t, ents[0], defaultGPSLatName, 1.5)
}

// junk and unrelated report classes must not disturb the current fix
func TestGPSIgnoresOtherClasses(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"SKY","satellites":[{"PRN":1,"used":true}]}`)
	f.send(`this is not json at all`)
	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":10}`)
	f.send(`{"class":"SKY","satellites":[]}`)
	fix := waitForFix(t, g)
	if fix.lat != 1.5 || fix.lon != 2.5 {
		t.Fatalf("bad fix %v", fix)
	}
}

// out of range coordinates are garbage and must be rejected without clobbering
// the fix we already have
func TestGPSRejectsBadCoordinates(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":10}`)
	waitForFix(t, g)
	f.send(`{"class":"TPV","mode":3,"lat":91.0,"lon":2.5}`)
	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":-181.0}`)
	// a mode 3 report with no coordinates at all
	f.send(`{"class":"TPV","mode":3}`)
	// land a known good fix so we know the bad ones were processed first
	f.send(`{"class":"TPV","mode":3,"lat":10.0,"lon":20.0,"altMSL":30}`)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if fix, _, _, _, _, ok := g.currentFix(); ok && fix.lat == 10.0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the trailing good fix")
}

// losing gpsd must invalidate the fix rather than leave us serving a location
// we can no longer confirm
func TestGPSConnectionLossInvalidates(t *testing.T) {
	f := newFakeGPSD(t)
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":10}`)
	waitForFix(t, g)
	f.Close()
	waitForNoFix(t, g)
}

// a dead gpsd must not stop entries from flowing
func TestGPSUnreachableServerPassthrough(t *testing.T) {
	// grab a port and immediately release it so nothing is listening
	ln, err := net.Listen(`tcp`, `127.0.0.1:0`)
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()

	g, err := NewGPSProcessor(GPSConfig{Server: addr})
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()

	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 1 || ents[0].EVB.Populated() {
		t.Fatal("expected a clean passthrough with no EVs")
	}
}

func TestGPSEmptyAndNilEntries(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":10}`)
	waitForFix(t, g)

	if ents, err := g.Process(nil); err != nil {
		t.Fatal(err)
	} else if len(ents) != 0 {
		t.Fatalf("bad entry count %d", len(ents))
	}

	// a nil entry in the block must be skipped rather than panic
	ents, err := g.Process([]*entry.Entry{nil, {Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	if len(ents) != 2 {
		t.Fatalf("bad entry count %d", len(ents))
	}
	checkFloatEV(t, ents[1], defaultGPSLatName, 1.5)
}

func TestGPSFlushAndDoubleClose(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)

	if ents := g.Flush(); ents != nil {
		t.Fatalf("Flush should not produce entries, got %d", len(ents))
	}
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
	// Close must be idempotent, ingesters tear down processor sets more than once
	if err := g.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestGPSConfigUpdate(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()
	g := newTestGPS(t, f, nil)
	defer g.Close()

	f.send(`{"class":"TPV","mode":3,"lat":1.5,"lon":2.5,"altMSL":10}`)
	waitForFix(t, g)

	// renaming the EVs takes effect without bouncing the gpsd connection
	if err := g.Config(GPSConfig{Server: f.addr(), Lat_Name: `Latitude`, Lon_Name: `Longitude`}); err != nil {
		t.Fatal(err)
	}
	ents, err := g.Process([]*entry.Entry{{Data: []byte(`testing`)}})
	if err != nil {
		t.Fatal(err)
	}
	checkFloatEV(t, ents[0], `Latitude`, 1.5)
	checkFloatEV(t, ents[0], `Longitude`, 2.5)

	if err := g.Config(nil); err != ErrNilConfig {
		t.Fatalf("expected ErrNilConfig, got %v", err)
	}
	if err := g.Config(`not a gps config`); err == nil {
		t.Fatal("expected an error on a bad config type")
	}
	if err := g.Config(GPSConfig{Server: f.addr(), Max_Fix_Age: `chicken`}); err != ErrGPSInvalidMaxAge {
		t.Fatalf("expected ErrGPSInvalidMaxAge, got %v", err)
	}
}

// the full ProcessorSet path, proving the processor is wired into the registry
func TestGPSProcessorSet(t *testing.T) {
	f := newFakeGPSD(t)
	defer f.Close()

	body := fmt.Sprintf("[preprocessor \"gps1\"]\ntype = gps\nServer = %s\n", f.addr())
	tc := struct {
		Preprocessor ProcessorConfig
	}{}
	if err := gcfg.ReadStringInto(&tc, body); err != nil {
		t.Fatal(err)
	}
	if err := tc.Preprocessor.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := tc.Preprocessor.CheckConfig(`gps1`); err != nil {
		t.Fatal(err)
	}
	p, err := tc.Preprocessor.ProcessorSet(&testTagWriter{}, []string{`gps1`})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()

	f.send(`{"class":"TPV","mode":3,"lat":44.0682,"lon":-114.7420,"altMSL":1875.5}`)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ent := &entry.Entry{Data: []byte(`testing`)}
		if err := p.Process(ent); err != nil {
			t.Fatal(err)
		}
		if v, ok := ent.GetEnumeratedValue(defaultGPSLatName); ok {
			if fv, ok := v.(float64); !ok || fv != 44.0682 {
				t.Fatalf("bad lat EV %v", v)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for the processor set to attach a fix")
}

// testTagWriter satisfies the tagWriter interface that ProcessorSet requires
type testTagWriter struct {
	testWriter
	testTagger
}

func checkFloatEV(t *testing.T, ent *entry.Entry, name string, want float64) {
	t.Helper()
	v, ok := ent.GetEnumeratedValue(name)
	if !ok {
		t.Fatalf("missing EV %q", name)
	}
	fv, ok := v.(float64)
	if !ok {
		t.Fatalf("EV %q is %T, want float64", name, v)
	}
	if fv != want {
		t.Fatalf("EV %q is %v, want %v", name, fv, want)
	}
}
