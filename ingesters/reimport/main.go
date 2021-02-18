/*************************************************************************
 * Copyright 2017 Gravwell, Inc. All rights reserved.
 * Contact: <legal@gravwell.io>
 *
 * This software may be modified and distributed under the terms of the
 * BSD 2-clause license. See the LICENSE file for details.
 **************************************************************************/

package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"github.com/gravwell/gravwell/v3/ingest"
	"github.com/gravwell/gravwell/v3/ingest/config"
	"github.com/gravwell/gravwell/v3/ingest/entry"
	"github.com/gravwell/gravwell/v3/ingesters/args"
	"github.com/gravwell/gravwell/v3/ingesters/utils"
	"github.com/gravwell/gravwell/v3/ingesters/version"
)

const (
	initBuffSize = 4 * 1024 * 1024
	maxBuffSize  = 128 * 1024 * 1024

	jsonFormat string = `json`
	csvFormat  string = `csv`
)

var (
	inFile     = flag.String("i", "", "Input file to process (specify - for stdin)")
	ver        = flag.Bool("version", false, "Print version and exit")
	verbose    = flag.Bool("v", false, "Print every step")
	status     = flag.Bool("status", false, "Output ingest rate stats as we go")
	srcOvr     = flag.String("source-override", "", "Override source with address, hash, or integeter")
	fmtF       = flag.String("import-format", "", "Set the import file format manually")
	tagOvr     = flag.String("tag-override", "", "Override the import file tags")
	rebaseTime = flag.Bool("rebase-timestamp", false, "Rewrite timestamps so the most recent entry is at the current time. (Warning: may be slow with large files!)")

	nlBytes     = []byte("\n")
	count       uint64
	totalBytes  uint64
	dur         time.Duration
	srcOverride net.IP

	timeDelta time.Duration // if we're rebasing, this is the adjustment added to each entry's TS

	format string
)

type itemReader interface {
	ReadEntry() (*entry.Entry, error)
	OverrideTags(tg entry.EntryTag)
}

func init() {
	flag.Parse()
	if *ver {
		version.PrintVersion(os.Stdout)
		ingest.PrintVersion(os.Stdout)
		os.Exit(0)
	}
	if *fmtF != `` {
		format = strings.ToLower(strings.TrimSpace(*fmtF))
	}
}

func main() {
	debug.SetTraceback("all")
	if *inFile == "" {
		log.Fatal("Input file path required")
	}
	a, err := args.Parse()
	if err != nil {
		log.Fatalf("Invalid arguments: %v\n", err)
	}
	if len(a.Tags) != 1 {
		log.Fatal("File oneshot only accepts a single tag")
	}

	if *srcOvr != `` {
		if srcOverride, err = config.ParseSource(*srcOvr); err != nil {
			log.Fatalf("Invalid source override")
		}
	}

	if *rebaseTime && *inFile == `-` {
		log.Fatal("Cannot rebase time when reading from stdin!")
	}

	if format == `` {
		//attempt to figure it out
		switch strings.ToLower(filepath.Ext(*inFile)) {
		case `.json`:
			format = jsonFormat
		case `.csv`:
			format = csvFormat
		default:
			log.Fatalf("Could not determine format of input file, please set -import-format")
		}
	}

	//fire up a uniform muxer
	igst, err := ingest.NewUniformIngestMuxer(a.Conns, a.Tags, a.IngestSecret, a.TLSPublicKey, a.TLSPrivateKey, "")
	if err != nil {
		log.Fatalf("Failed to create new ingest muxer: %v\n", err)
	}
	if err := igst.Start(); err != nil {
		log.Fatalf("Failed to start ingest muxer: %v\n", err)
	}
	if err := igst.WaitForHot(a.Timeout); err != nil {
		log.Fatalf("Failed to wait for hot connection: %v\n", err)
	}
	if len(a.Conns) > 0 {
		//sleep so that all connections can get a crack at negotiating tags
		time.Sleep(500 * time.Millisecond)
	}

	//get a handle on the input file with a wrapped decompressor if needed
	var fin io.ReadCloser
	if *inFile == "-" {
		fin = os.Stdin
	} else {
		fin, err = utils.OpenBufferedFileReader(*inFile, 8192)
		if err != nil {
			log.Fatalf("Failed to open %s: %v\n", *inFile, err)
		}
	}
	var ir itemReader
	ir, err = getReader(fin, igst)
	if err != nil {
		igst.Close()
		log.Fatal(err)
	}

	if *rebaseTime {
		var newest *entry.Entry
		var ent *entry.Entry
		for {
			if ent, err = ir.ReadEntry(); err != nil {
				if err == io.EOF {
					err = nil
				}
				break
			}
			if newest == nil || ent.TS.After(newest.TS) {
				newest = ent
			}
		}
		if err != nil {
			log.Fatalf("Couldn't read full input file: %v", err)
		}
		timeDelta = time.Now().Sub(newest.TS.StandardTime())
		fmt.Printf("timeDelta = %v\n", timeDelta)
		// Now reset the reader
		fin, err = utils.OpenBufferedFileReader(*inFile, 8192)
		if err != nil {
			log.Fatalf("Failed to open %s: %v\n", *inFile, err)
		}
		ir, err = getReader(fin, igst)
		if err != nil {
			igst.Close()
			log.Fatal(err)
		}
	}

	if *tagOvr != `` {
		tag, err := igst.NegotiateTag(*tagOvr)
		if err != nil {
			igst.Close()
			log.Fatalf("Failed to negotiate the override tag %s: %v", *tagOvr, err)
		}
		ir.OverrideTags(tag)
	}

	//go ingest the file
	if err := doIngest(ir, igst); err != nil {
		log.Fatalf("Failed to ingest file: %v\n", err)
	}

	if err = igst.Sync(a.Timeout); err != nil {
		log.Fatalf("Failed to sync ingest muxer: %v\n", err)
	}
	if err := igst.Close(); err != nil {
		log.Fatalf("Failed to close the ingest muxer: %v\n", err)
	}
	if err := fin.Close(); err != nil {
		log.Fatalf("Failed to close the input file: %v\n", err)
	}
	fmt.Printf("Completed in %v (%s)\n", dur, ingest.HumanSize(totalBytes))
	fmt.Printf("Total Count: %s\n", ingest.HumanCount(count))
	fmt.Printf("Entry Rate: %s\n", ingest.HumanEntryRate(count, dur))
	fmt.Printf("Ingest Rate: %s\n", ingest.HumanRate(totalBytes, dur))
}

func getReader(fin io.ReadCloser, igst *ingest.IngestMuxer) (ir itemReader, err error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case csvFormat:
		if ir, err = newCSVReader(fin, igst); err != nil {
			err = fmt.Errorf("Failed to make CSV reader: %v\n", err)
		}
	case jsonFormat:
		if ir, err = newJSONReader(fin, igst); err != nil {
			err = fmt.Errorf("Failed to make JSON reader: %v\n", err)
		}
	default:
		err = fmt.Errorf("Invalid format %v\n", format)
	}
	return
}

func doIngest(ir itemReader, igst *ingest.IngestMuxer) (err error) {
	//if not doing regular updates, just fire it off
	if !*status {
		err = doImport(ir, igst)
		return
	}

	errCh := make(chan error, 1)
	tckr := time.NewTicker(time.Second)
	defer tckr.Stop()
	go func(ch chan error) {
		ch <- doImport(ir, igst)
	}(errCh)

loop:
	for {
		lastts := time.Now()
		lastcnt := count
		lastsz := totalBytes
		select {
		case err = <-errCh:
			fmt.Println("\nDONE")
			break loop
		case _ = <-tckr.C:
			dur := time.Since(lastts)
			cnt := count - lastcnt
			bts := totalBytes - lastsz
			fmt.Printf("\r%s %s                                     ",
				ingest.HumanEntryRate(cnt, dur),
				ingest.HumanRate(bts, dur))
		}
	}
	return
}

func doImport(ir itemReader, igst *ingest.IngestMuxer) (err error) {
	var ent *entry.Entry
	src := srcOverride
	if src == nil {
		if src, err = igst.SourceIP(); err != nil {
			return
		}
	}

	start := time.Now()
	for {
		if ent, err = ir.ReadEntry(); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		if srcOverride != nil {
			ent.SRC = srcOverride
		}
		if ent.SRC == nil {
			ent.SRC = src
		}
		if timeDelta != 0 {
			ent.TS = ent.TS.Add(timeDelta)
		}
		if err = igst.WriteEntry(ent); err != nil {
			break
		}
		if *verbose {
			fmt.Println(ent.TS, ent.Tag, ent.SRC, string(ent.Data))
		}
		count++
		totalBytes += uint64(len(ent.Data))
	}
	dur = time.Since(start)
	return
}
