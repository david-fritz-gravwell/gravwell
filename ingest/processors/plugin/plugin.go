// +build !386,!arm,!mips,!mipsle,!s390x

/*************************************************************************
 * Copyright 2018 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package plugin

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"reflect"
	"sync"
	"time"

	"github.com/gravwell/gravwell/v3/ingest/config"
	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/open2b/scriggo"
	"github.com/open2b/scriggo/native"
)

var (
	ErrInvalidScript = errors.New("invalid plugin program")
	ErrNotReady      = errors.New("not ready")

	packages native.Packages
)

const (
	BuiltinPackageName string = `gravwell`
	ConfigMapName      string = `ConfigMap`
	TaggerName         string = `Tagger`
	ExecuteFuncName    string = `Execute`
	ConfigFuncName     string = `Config`
	FlushFuncName      string = `Flush`
	ProcessFuncName    string = `Process`
)

type pluginState int

const (
	bad        pluginState = -1
	fresh      pluginState = 0
	built      pluginState = 1
	running    pluginState = 2
	registered pluginState = 3
	done       pluginState = 4
)

func NewPluginProgram(content []byte) (pp *PluginProgram, err error) {
	if len(content) == 0 {
		err = ErrInvalidScript
		return
	}
	fsys := scriggo.Files{`main.go`: content}
	pp, err = NewPlugin(fsys)
	return
}

func NewPlugin(fsys fs.FS) (pp *PluginProgram, err error) {
	ppTemp := &PluginProgram{
		rc: make(chan error, 1),
		dc: make(chan error, 1),
	}
	ppTemp.ctx, ppTemp.cancel = context.WithCancel(context.Background())
	if err = buildProgram(fsys, ppTemp); err != nil {
		return
	}
	pp = ppTemp
	return
}

func buildProgram(fsys fs.FS, pp *PluginProgram) (err error) {
	defer buildCatcher(&err) //catch the nasties
	local := native.Packages{
		BuiltinPackageName: native.Package{
			Name:         BuiltinPackageName,
			Declarations: builtinItems(pp),
		},
	}
	opts := scriggo.BuildOptions{
		AllowGoStmt: true,
		Packages:    native.CombinedImporter{packages, local},
	}

	// Build the program.
	if pp.pgrm, err = scriggo.Build(fsys, &opts); err != nil {
		pp.pgrm = nil
		pp.setState(bad)
	} else {
		pp.setState(built)
	}
	return
}

type ConfigMap interface {
	Names() []string
	GetBool(string) (bool, error)
	GetInt(string) (int64, error)
	GetUint(string) (uint64, error)
	GetFloat(string) (float64, error)
	GetString(string) (string, error)
	GetStringSlice(string) ([]string, error)
}

// this is a copy of the interface in processors, but we can't import processors due to import cycles
type Tagger interface {
	NegotiateTag(name string) (entry.EntryTag, error)
	LookupTag(entry.EntryTag) (string, bool)
	KnownTags() []string
}

type ConfigFunc func(ConfigMap, Tagger) error
type FlushFunc func() []*entry.Entry
type ProcessFunc func([]*entry.Entry) ([]*entry.Entry, error)

type PluginProgram struct {
	sync.WaitGroup
	sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	cf         ConfigFunc
	ff         FlushFunc
	pf         ProcessFunc
	name       string
	rc         chan error // signal channel to tell the outside world that the plugin has registered
	dc         chan error // signal channel to tell the outside world that the program finished
	pgrm       *scriggo.Program
	state      pluginState
	err        error
	registered bool
}

func (pp *PluginProgram) setState(v pluginState) {
	pp.Lock()
	pp.state = v
	pp.Unlock()
}

func (pp *PluginProgram) getState() (v pluginState) {
	pp.Lock()
	v = pp.state
	pp.Unlock()
	return
}

func (pp *PluginProgram) register(name string, cf ConfigFunc, pf ProcessFunc, ff FlushFunc) (err error) {
	if name == `` {
		err = errors.New("invalid name")
		return
	} else if cf == nil || pf == nil || ff == nil {
		err = errors.New("invalid parameters")
		return
	}
	pp.Lock()
	alreadyRegistered := pp.registered
	pp.Unlock()

	if alreadyRegistered {
		err = errors.New("already registered")
		return
	}
	pp.name = name
	pp.cf, pp.pf, pp.ff = cf, pf, ff
	pp.setState(registered)
	pp.rc <- nil
	close(pp.rc)
	pp.Wait()
	return
}

// Run will execute the underlying program and blocks for timeout time,
// it is waiting for the program to come up and register itself
// if it does not, then we kill it
func (pp *PluginProgram) Run(to time.Duration) (err error) {
	if pp.getState() != built {
		return errors.New("bad program state")
	}
	go pp.execute()
	select {
	case err = <-pp.rc:
	case err = <-pp.dc:
		err = fmt.Errorf("program exited before registration: %w", err)
	case <-time.After(to):
		err = errors.New("Timed out waiting for program to register")
		pp.cancel()
	}
	return
}

func (pp *PluginProgram) Close() (err error) {
	if s := pp.getState(); s != running {
		if s == bad || s == done {
			err = pp.err
		}
		return
	}
	pp.Done()
	time.Sleep(250 * time.Millisecond) //let the program close out
	pp.cancel()                        //go down hard
	if err = <-pp.dc; err == nil {
		err = pp.err
	}
	return
}

func (pp *PluginProgram) Config(vc *config.VariableConfig, tg Tagger) error {
	if pp == nil || pp.cf == nil {
		return ErrNotReady
	} else if st := pp.getState(); st != registered {
		return fmt.Errorf("bad state, %s != %s", st, registered)
	}
	return pp.cf(vc, tg)
}

func (pp *PluginProgram) Flush() []*entry.Entry {
	if pp == nil || pp.cf == nil {
		return nil
	} else if st := pp.getState(); st != registered {
		return nil
	}

	return pp.ff()
}

func (pp *PluginProgram) Process(ents []*entry.Entry) ([]*entry.Entry, error) {
	if pp == nil || pp.cf == nil {
		return nil, ErrNotReady
	} else if st := pp.getState(); st != registered {
		return nil, fmt.Errorf("bad state, %s != %s", st, running)
	}

	return pp.pf(ents)
}

// Ready indicates if the program is running and has registered all the things we need
func (pp *PluginProgram) Ready() (r bool) {
	r = pp.getState() == registered
	return
}

// execute gets the actual plugin program up and running
func (pp *PluginProgram) execute() {
	if pp.pgrm == nil {
		pp.err = errors.New("plugin not compiled")
		pp.rc <- pp.err
		pp.setState(bad)
		return
	}
	defer execCatcher(pp.dc) //catch the nasties
	opts := scriggo.RunOptions{
		Context: pp.ctx,
	}
	pp.Add(1)
	pp.setState(running)
	pp.err = pp.pgrm.Run(&opts)
	if pp.err != nil {
		pp.setState(bad)
	} else {
		pp.setState(done)
	}
	pp.dc <- pp.err
	close(pp.dc)
}

func builtinItems(pp *PluginProgram) native.Declarations {
	decs := make(native.Declarations, 2)
	decs[ExecuteFuncName] = pp.register
	decs[ConfigFuncName] = reflect.TypeOf((*ConfigFunc)(nil)).Elem()
	decs[FlushFuncName] = reflect.TypeOf((*FlushFunc)(nil)).Elem()
	decs[ProcessFuncName] = reflect.TypeOf((*ProcessFunc)(nil)).Elem()
	decs[ConfigMapName] = reflect.TypeOf((*ConfigMap)(nil)).Elem()
	decs[TaggerName] = reflect.TypeOf((*Tagger)(nil)).Elem()
	return decs
}

func buildCatcher(err *error) {
	if r := recover(); r != nil {
		*err = fmt.Errorf("Critical Error, failed to build program - %v", r)
	}
}

func execCatcher(dc chan error) {
	if r := recover(); r != nil {
		if dc != nil {
			dc <- fmt.Errorf("Critical Error, failed to execute program - %v", r)
		}
	}
}

func (ps pluginState) String() string {
	switch ps {
	case bad:
		return `bad`
	case fresh:
		return `fresh`
	case built:
		return `built`
	case running:
		return `running`
	case registered:
		return `registered`
	case done:
		return `done`
	}
	return `unknown`
}
