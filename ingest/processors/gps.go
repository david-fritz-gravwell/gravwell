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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/gravwell/gravwell/v3/ingest/config"
	"github.com/gravwell/gravwell/v3/ingest/entry"
)

const (
	GPSProcessor string = `gps`

	defaultGPSServer  string = `127.0.0.1:2947`
	defaultGPSPort    string = `2947`
	defaultGPSMaxAge         = 30 * time.Second
	defaultGPSLatName string = `Lat`
	defaultGPSLonName string = `Lon`
	defaultGPSAltName string = `Alt`

	// gpsd will happily sit idle when a receiver has no sky view, so we only
	// use the dial timeout to bound connection establishment, not reads.
	gpsdDialTimeout = 5 * time.Second

	// reconnect backoff bounds, gpsd is usually local so we retry fairly hard
	gpsdMinBackoff = time.Second
	gpsdMaxBackoff = 30 * time.Second

	// gpsd JSON reports are small, but give the scanner room for verbose
	// SKY reports which enumerate every satellite in view
	gpsdMaxLine = 256 * 1024

	// watch command asks gpsd to stream JSON reports for every device
	gpsdWatchCommand = "?WATCH={\"enable\":true,\"json\":true};\n"

	// gpsd fix modes from the TPV "mode" field: 0 unknown, 1 no fix,
	// 2 is a 2D fix (lat/lon only), 3 is a 3D fix (adds altitude)
	gpsdModeNoFix int = 1
	gpsdMode3D    int = 3
)

var (
	ErrGPSMissingServer   = errors.New("missing server in gps config")
	ErrGPSInvalidServer   = errors.New("invalid server in gps config")
	ErrGPSInvalidMaxAge   = errors.New("invalid Max-Fix-Age in gps config")
	ErrGPSInvalidEVName   = errors.New("invalid enumerated value name in gps config")
	ErrGPSDuplicateEVName = errors.New("duplicate enumerated value name in gps config")
)

// GPSConfig controls the gps preprocessor, which maintains a connection to a
// gpsd daemon and attaches the current location to entries as intrinsic EVs.
type GPSConfig struct {
	Server           string // gpsd address, host or host:port (default 127.0.0.1:2947)
	Max_Fix_Age      string // discard fixes older than this, "0" disables the check (default 30s)
	Lat_Name         string // EV name for latitude (default Lat)
	Lon_Name         string // EV name for longitude (default Lon)
	Alt_Name         string // EV name for altitude (default Alt)
	Disable_Altitude bool   // do not attach an altitude EV even when a 3D fix is available
	Require_3D_Fix   bool   // ignore 2D fixes, which have no altitude component

	maxAge time.Duration
}

func GPSLoadConfig(vc *config.VariableConfig) (c GPSConfig, err error) {
	if err = vc.MapTo(&c); err == nil {
		err = c.validate()
	}
	return
}

// validate normalizes the config in place and reports anything unusable.
func (c *GPSConfig) validate() (err error) {
	if c.Server = strings.TrimSpace(c.Server); c.Server == `` {
		c.Server = defaultGPSServer
	} else if c.Server, err = normalizeGPSServer(c.Server); err != nil {
		return
	}

	if c.Max_Fix_Age = strings.TrimSpace(c.Max_Fix_Age); c.Max_Fix_Age == `` {
		c.maxAge = defaultGPSMaxAge
	} else if c.maxAge, err = time.ParseDuration(c.Max_Fix_Age); err != nil {
		return ErrGPSInvalidMaxAge
	} else if c.maxAge < 0 {
		return ErrGPSInvalidMaxAge
	}

	if c.Lat_Name = strings.TrimSpace(c.Lat_Name); c.Lat_Name == `` {
		c.Lat_Name = defaultGPSLatName
	}
	if c.Lon_Name = strings.TrimSpace(c.Lon_Name); c.Lon_Name == `` {
		c.Lon_Name = defaultGPSLonName
	}
	if c.Alt_Name = strings.TrimSpace(c.Alt_Name); c.Alt_Name == `` {
		c.Alt_Name = defaultGPSAltName
	}
	for _, name := range []string{c.Lat_Name, c.Lon_Name, c.Alt_Name} {
		if len(name) > entry.MaxEvNameLength {
			return ErrGPSInvalidEVName
		}
	}
	if c.Lat_Name == c.Lon_Name {
		return ErrGPSDuplicateEVName
	}
	if !c.Disable_Altitude && (c.Alt_Name == c.Lat_Name || c.Alt_Name == c.Lon_Name) {
		return ErrGPSDuplicateEVName
	}
	return nil
}

// normalizeGPSServer accepts a bare host or a host:port and always returns a
// host:port, defaulting to the gpsd well known port.
func normalizeGPSServer(v string) (string, error) {
	if _, _, err := net.SplitHostPort(v); err == nil {
		return v, nil
	}
	// no port, tack on the default and make sure the result is sane
	withPort := net.JoinHostPort(v, defaultGPSPort)
	if _, _, err := net.SplitHostPort(withPort); err != nil {
		return ``, ErrGPSInvalidServer
	}
	return withPort, nil
}

// gpsFix is a single position report from gpsd along with the local time we
// received it.  We deliberately track local receipt time rather than the
// timestamp gpsd reports so that a receiver with a bad clock cannot make a
// stale fix look fresh.
type gpsFix struct {
	lat    float64
	lon    float64
	alt    float64
	hasAlt bool
	rx     time.Time
}

// tpvReport is the subset of the gpsd TPV (time-position-velocity) class that
// we care about.  altMSL is the modern height above mean sea level field, alt
// is the deprecated alias that older gpsd releases emit.
type tpvReport struct {
	Class  string   `json:"class"`
	Mode   int      `json:"mode"`
	Lat    *float64 `json:"lat"`
	Lon    *float64 `json:"lon"`
	AltMSL *float64 `json:"altMSL"`
	Alt    *float64 `json:"alt"`
}

// GPS attaches the current gpsd location to entries as intrinsic EVs.  A
// background goroutine owns the gpsd connection so that Process never blocks
// on the network; when no usable fix is available entries pass through
// untouched.
type GPS struct {
	GPSConfig

	mtx   sync.Mutex
	fix   gpsFix
	valid bool
	conn  net.Conn
	done  chan struct{}
	wg    sync.WaitGroup
	once  sync.Once
}

func NewGPSProcessor(cfg GPSConfig) (*GPS, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	g := &GPS{
		GPSConfig: cfg,
		done:      make(chan struct{}),
	}
	g.wg.Add(1)
	go g.run()
	return g, nil
}

func (g *GPS) Config(v interface{}) (err error) {
	if v == nil {
		return ErrNilConfig
	}
	cfg, ok := v.(GPSConfig)
	if !ok {
		return fmt.Errorf("Invalid configuration, unknown type %T", v)
	}
	if err = cfg.validate(); err != nil {
		return
	}
	g.mtx.Lock()
	restart := cfg.Server != g.Server
	g.GPSConfig = cfg
	conn := g.conn
	g.mtx.Unlock()

	// only bounce the connection if we are pointed at a different gpsd, EV
	// naming changes take effect on the next Process call for free
	if restart && conn != nil {
		conn.Close() // the run loop will notice and redial the new server
	}
	return nil
}

// run maintains the gpsd connection for the life of the processor, redialing
// with backoff whenever gpsd goes away.
func (g *GPS) run() {
	defer g.wg.Done()
	backoff := gpsdMinBackoff
	for {
		select {
		case <-g.done:
			return
		default:
		}
		if err := g.session(); err == nil {
			// clean session, reset backoff so a gpsd restart reconnects fast
			backoff = gpsdMinBackoff
		}
		select {
		case <-g.done:
			return
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > gpsdMaxBackoff {
			backoff = gpsdMaxBackoff
		}
	}
}

// session dials gpsd, asks it to stream JSON, and consumes reports until the
// connection fails or the processor is closed.
func (g *GPS) session() error {
	g.mtx.Lock()
	server := g.Server
	g.mtx.Unlock()

	conn, err := net.DialTimeout(`tcp`, server, gpsdDialTimeout)
	if err != nil {
		return err
	}

	// publish the connection so Close can unblock the read below
	g.mtx.Lock()
	select {
	case <-g.done:
		// raced with Close, do not install the connection
		g.mtx.Unlock()
		conn.Close()
		return nil
	default:
	}
	g.conn = conn
	g.mtx.Unlock()

	defer func() {
		g.mtx.Lock()
		if g.conn == conn {
			g.conn = nil
		}
		g.mtx.Unlock()
		conn.Close()
		// a dead connection means a dead fix, do not keep serving a
		// location we can no longer confirm
		g.invalidate()
	}()

	if _, err = conn.Write([]byte(gpsdWatchCommand)); err != nil {
		return err
	}

	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 0, bufio.MaxScanTokenSize), gpsdMaxLine)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var tpv tpvReport
		if err := json.Unmarshal(line, &tpv); err != nil {
			continue // gpsd emits classes we do not model, skip anything unparsable
		}
		if tpv.Class != `TPV` {
			continue
		}
		g.handleTPV(tpv)
	}
	return sc.Err()
}

// handleTPV converts a gpsd position report into a fix, invalidating the
// current location whenever the receiver reports that it has lost its fix.
func (g *GPS) handleTPV(tpv tpvReport) {
	g.mtx.Lock()
	require3D := g.Require_3D_Fix
	g.mtx.Unlock()

	if tpv.Mode <= gpsdModeNoFix || (require3D && tpv.Mode < gpsdMode3D) {
		g.invalidate()
		return
	}
	if tpv.Lat == nil || tpv.Lon == nil {
		return // a fix without coordinates tells us nothing, leave the last one alone
	}
	lat, lon := *tpv.Lat, *tpv.Lon
	if !validCoordinate(lat, lon) {
		return
	}
	fix := gpsFix{
		lat: lat,
		lon: lon,
		rx:  time.Now(),
	}
	// altitude only exists on a 3D fix, prefer altMSL and fall back to the
	// deprecated alt field for older gpsd releases
	if tpv.Mode >= gpsdMode3D {
		if alt := tpv.AltMSL; alt != nil && !math.IsNaN(*alt) && !math.IsInf(*alt, 0) {
			fix.alt, fix.hasAlt = *alt, true
		} else if alt = tpv.Alt; alt != nil && !math.IsNaN(*alt) && !math.IsInf(*alt, 0) {
			fix.alt, fix.hasAlt = *alt, true
		}
	}

	g.mtx.Lock()
	g.fix, g.valid = fix, true
	g.mtx.Unlock()
}

func validCoordinate(lat, lon float64) bool {
	if math.IsNaN(lat) || math.IsNaN(lon) || math.IsInf(lat, 0) || math.IsInf(lon, 0) {
		return false
	}
	return lat >= -90.0 && lat <= 90.0 && lon >= -180.0 && lon <= 180.0
}

func (g *GPS) invalidate() {
	g.mtx.Lock()
	g.valid = false
	g.mtx.Unlock()
}

// currentFix returns the active fix, or false when there is nothing usable to
// attach because we never got a fix, lost it, or it has gone stale.
func (g *GPS) currentFix() (fix gpsFix, latName, lonName, altName string, wantAlt, ok bool) {
	g.mtx.Lock()
	defer g.mtx.Unlock()
	if !g.valid {
		return
	}
	if g.maxAge > 0 && time.Since(g.fix.rx) > g.maxAge {
		return
	}
	fix = g.fix
	latName, lonName, altName = g.Lat_Name, g.Lon_Name, g.Alt_Name
	wantAlt = !g.Disable_Altitude && fix.hasAlt
	ok = true
	return
}

// Process attaches the current location to each entry.  Entries always pass
// through: a missing fix or an entry that has no room left for EVs is never a
// reason to stall or drop ingest.
func (g *GPS) Process(ents []*entry.Entry) ([]*entry.Entry, error) {
	if len(ents) == 0 {
		return ents, nil
	}
	fix, latName, lonName, altName, wantAlt, ok := g.currentFix()
	if !ok {
		return ents, nil
	}
	for _, ent := range ents {
		if ent == nil {
			continue
		}
		// an EV that will not fit is skipped rather than surfaced as an
		// error, which would tear down the rest of the processor chain
		if err := ent.AddEnumeratedValueEx(latName, fix.lat); err != nil {
			continue
		}
		if err := ent.AddEnumeratedValueEx(lonName, fix.lon); err != nil {
			continue
		}
		if wantAlt {
			ent.AddEnumeratedValueEx(altName, fix.alt)
		}
	}
	return ents, nil
}

func (g *GPS) Flush() []*entry.Entry {
	return nil
}

func (g *GPS) Close() error {
	g.once.Do(func() {
		g.mtx.Lock()
		close(g.done)
		conn := g.conn
		g.mtx.Unlock()
		if conn != nil {
			conn.Close() // unblock the scanner so the run loop can exit
		}
		g.wg.Wait()
	})
	return nil
}
