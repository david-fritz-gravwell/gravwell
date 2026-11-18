/*************************************************************************
 * Copyright 2026 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

// The openconfig/gnmi module can render a gnmi.Path as strings but has no
// parser for the xpath-ish notation that every vendor's documentation and
// every operator's fingers use, so we carry a small one here.

// parsePath converts an xpath style gNMI path into a gnmi.Path.
//
// Accepted forms:
//
//	/interfaces/interface/state/counters
//	/interfaces/interface[name=eth0]/state
//	/network-instances/network-instance[name=default][type=L3VRF]/protocols
//	openconfig:/interfaces/interface/state
//
// A backslash escapes the following character, so an element name or key
// value containing a literal /, [, ], =, : or \ is written with it escaped.
func parsePath(s string) (*gnmipb.Path, error) {
	orig := strings.TrimSpace(s)
	s = orig
	if s == `` {
		return nil, errors.New("empty path")
	}

	p := &gnmipb.Path{}

	// An origin, if present, is everything ahead of the first unescaped colon
	// that occurs before the first unescaped slash.
	if i := indexUnescaped(s, ':', '/'); i >= 0 {
		origin, _, err := scanToken(s[:i], ``)
		if err != nil {
			return nil, fmt.Errorf("path %q has an invalid origin: %v", orig, err)
		} else if origin == `` {
			return nil, fmt.Errorf("path %q has an empty origin", orig)
		}
		p.Origin = origin
		s = s[i+1:]
	}

	if s == `` || s == `/` {
		return p, nil // the root
	} else if !strings.HasPrefix(s, `/`) {
		return nil, fmt.Errorf("path %q must begin with /", orig)
	}
	s = s[1:]

	for s != `` {
		name, rest, err := scanToken(s, `/[`)
		if err != nil {
			return nil, fmt.Errorf("path %q is invalid: %v", orig, err)
		} else if name == `` {
			return nil, fmt.Errorf("path %q contains an empty element", orig)
		}
		elem := &gnmipb.PathElem{Name: name}
		s = rest

		// zero or more [key=value] predicates
		for strings.HasPrefix(s, `[`) {
			s = s[1:]
			var k, v string
			if k, s, err = scanToken(s, `=]`); err != nil {
				return nil, fmt.Errorf("path %q is invalid: %v", orig, err)
			} else if !strings.HasPrefix(s, `=`) {
				return nil, fmt.Errorf("path %q has a key with no = in element %q", orig, elem.Name)
			} else if k == `` {
				return nil, fmt.Errorf("path %q has an empty key name in element %q", orig, elem.Name)
			}
			if v, s, err = scanToken(s[1:], `]`); err != nil {
				return nil, fmt.Errorf("path %q is invalid: %v", orig, err)
			} else if !strings.HasPrefix(s, `]`) {
				return nil, fmt.Errorf("path %q has an unterminated [ in element %q", orig, elem.Name)
			}
			s = s[1:]
			if elem.Key == nil {
				elem.Key = map[string]string{}
			} else if _, dup := elem.Key[k]; dup {
				return nil, fmt.Errorf("path %q repeats key %q in element %q", orig, k, elem.Name)
			}
			elem.Key[k] = v
		}
		p.Elem = append(p.Elem, elem)

		if s == `` {
			break
		} else if !strings.HasPrefix(s, `/`) {
			return nil, fmt.Errorf("path %q has trailing garbage %q after element %q", orig, s, elem.Name)
		} else if s = s[1:]; s == `` {
			return nil, fmt.Errorf("path %q has a trailing /", orig)
		}
	}

	return p, nil
}

// formatElems renders path elements in xpath notation with no leading slash.
// This is what we use for the keys of the values object on an entry, so that
// a leaf sitting directly under the notification prefix comes out as a bare
// name like "in-octets" rather than a fully qualified path.
func formatElems(elems []*gnmipb.PathElem) string {
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		if e == nil {
			continue
		}
		var b strings.Builder
		b.WriteString(escapePathToken(e.GetName()))
		if keys := e.GetKey(); len(keys) != 0 {
			names := make([]string, 0, len(keys))
			for k := range keys {
				names = append(names, k)
			}
			// map iteration order is random, so sort to keep entries stable
			sort.Strings(names)
			for _, k := range names {
				fmt.Fprintf(&b, "[%s=%s]", escapePathToken(k), escapePathToken(keys[k]))
			}
		}
		parts = append(parts, b.String())
	}
	return strings.Join(parts, `/`)
}

// formatPath renders a full gNMI path, leading slash and origin included.
func formatPath(p *gnmipb.Path) string {
	if p == nil {
		return `/`
	}
	s := `/` + formatElems(p.GetElem())
	if o := p.GetOrigin(); o != `` {
		s = escapePathToken(o) + `:` + s
	}
	return s
}

// scanToken consumes s up to the first unescaped byte in delims, returning the
// unescaped token and the remainder of s beginning at that delimiter. An empty
// delims consumes and unescapes all of s.
func scanToken(s, delims string) (tok, rest string, err error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\':
			if i+1 >= len(s) {
				return ``, ``, errors.New(`dangling \ at end of path`)
			}
			i++
			b.WriteByte(s[i])
		case strings.IndexByte(delims, c) >= 0:
			return b.String(), s[i:], nil
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), ``, nil
}

// indexUnescaped returns the offset of the first unescaped target byte, or -1
// if an unescaped stop byte or the end of the string is reached first.
func indexUnescaped(s string, target, stop byte) int {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\\':
			i++
		case target:
			return i
		case stop:
			return -1
		}
	}
	return -1
}

// escapePathToken is the inverse of the unescaping done by scanToken.
func escapePathToken(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		switch c := s[i]; c {
		case '\\', '/', '[', ']', '=', ':':
			b.WriteByte('\\')
			b.WriteByte(c)
		default:
			b.WriteByte(c)
		}
	}
	return b.String()
}
