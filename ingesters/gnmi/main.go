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
	"fmt"
	"os"
	"sort"
	"sync"

	// Embed tzdata so that we don't rely on potentially broken timezone DBs on the host
	_ "time/tzdata"

	"github.com/gravwell/gravwell/v3/debug"
	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/gravwell/gravwell/v3/ingest/log"
	"github.com/gravwell/gravwell/v3/ingest/processors"
	"github.com/gravwell/gravwell/v3/ingesters/base"
	"github.com/gravwell/gravwell/v3/ingesters/utils"
)

const (
	defaultConfigLoc  = `/opt/gravwell/etc/gnmi.conf`
	defaultConfigDLoc = `/opt/gravwell/etc/gnmi.conf.d`
	ingesterName      = `gNMI Ingester`
	appName           = `gnmi`
)

func main() {
	go debug.HandleDebugSignals(ingesterName)
	var wg sync.WaitGroup
	var cfg *cfgType
	ibc := base.IngesterBaseConfig{
		IngesterName:                 ingesterName,
		AppName:                      appName,
		DefaultConfigLocation:        defaultConfigLoc,
		DefaultConfigOverlayLocation: defaultConfigDLoc,
		GetConfigFunc:                GetConfig,
	}
	ib, err := base.Init(ibc)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to get configuration %v\n", err)
		return
	} else if err = ib.AssignConfig(&cfg); err != nil || cfg == nil {
		fmt.Fprintf(os.Stderr, "failed to assign configuration %v %v\n", err, cfg == nil)
		return
	}

	igst, err := ib.GetMuxer()
	if err != nil {
		ib.Logger.FatalCode(0, "failed to get ingest connection", log.KVErr(err))
		return
	}
	defer igst.Close()
	ib.AnnounceStartup()

	exitCtx, exitFn := context.WithCancel(context.Background())

	// Iterate in a stable order so that startup logs are reproducible.
	names := make([]string, 0, len(cfg.Target))
	for name := range cfg.Target {
		names = append(names, name)
	}
	sort.Strings(names)

	// Held so they can be closed at shutdown; ProcessorSet.Close is what
	// flushes a buffering preprocessor and writes out what it still holds.
	procs := make([]*processors.ProcessorSet, 0, len(names))

	for _, name := range names {
		tcfg := cfg.Target[name]
		var tag entry.EntryTag
		var proc *processors.ProcessorSet
		if tag, err = igst.GetTag(tcfg.Tag_Name); err != nil {
			ib.Logger.FatalCode(0, "failed to get established tag",
				log.KV("tag", tcfg.Tag_Name),
				log.KV("target", name), log.KVErr(err))
		} else if proc, err = cfg.Preprocessor.ProcessorSet(igst, tcfg.Preprocessor); err != nil {
			ib.Logger.FatalCode(0, "preprocessor failure",
				log.KV("target", name), log.KVErr(err))
		}
		procs = append(procs, proc)

		gt, err := newGNMITarget(name, tcfg, tag, proc, ib.Logger)
		if err != nil {
			ib.Logger.FatalCode(0, "failed to build target",
				log.KV("target", name), log.KVErr(err))
		}

		if !tcfg.useTLS() {
			// Credentials ride in per-RPC metadata, so without TLS they are
			// on the wire in the clear. That is a deliberate choice here, but
			// it should never be a silent one.
			ib.Logger.Warn("gnmi target configured without TLS, credentials will be sent in cleartext",
				log.KV("target", name), log.KV("host", gt.dial))
		}

		ib.Logger.Info("starting gnmi target",
			log.KV("target", name), log.KV("host", gt.dial),
			log.KV("tag", tcfg.Tag_Name),
			log.KV("mode", tcfg.listMode().String()),
			log.KV("paths", len(tcfg.Subscribe_Path)))

		wg.Add(1)
		go gt.run(exitCtx, &wg)
	}

	ib.Debug("Running\n")

	//listen for signals so we can close gracefully
	utils.WaitForQuit()
	ib.AnnounceShutdown()

	exitFn()

	// wait for graceful shutdown
	wg.Wait()

	// Every target has stopped producing, so flush the preprocessors before
	// syncing; anything a buffering processor is still holding is written by
	// Close and would otherwise be dropped on the floor.
	for i, proc := range procs {
		if err := proc.Close(); err != nil {
			ib.Logger.Error("failed to close processors",
				log.KV("target", names[i]), log.KVErr(err))
		}
	}

	if err := igst.Sync(utils.ExitSyncTimeout); err != nil {
		ib.Logger.Error("failed to sync", log.KVErr(err))
	}
	if err := igst.Close(); err != nil {
		ib.Logger.Error("failed to close", log.KVErr(err))
	}
}
