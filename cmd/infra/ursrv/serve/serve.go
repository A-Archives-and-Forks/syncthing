// Copyright (C) 2018 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package serve

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/puzpuzpuz/xsync/v3"

	"github.com/syncthing/syncthing/lib/build"
	"github.com/syncthing/syncthing/lib/geoip"
	"github.com/syncthing/syncthing/lib/s3"
	"github.com/syncthing/syncthing/lib/ur/contract"
)

type CLI struct {
	Listen          string        `env:"UR_LISTEN_METRICS" help:"Usage reporting & metrics endpoint listen address" default:"0.0.0.0:8080"`
	ListenInternal  string        `env:"UR_LISTEN_INTERNAL" help:"Internal metrics endpoint listen address" default:"0.0.0.0:8082"`
	GeoIPLicenseKey string        `env:"UR_GEOIP_LICENSE_KEY"`
	GeoIPAccountID  int           `env:"UR_GEOIP_ACCOUNT_ID"`
	DumpFile        string        `env:"UR_DUMP_FILE" default:"reports.jsons"`
	DumpInterval    time.Duration `env:"UR_DUMP_INTERVAL" default:"5m"`

	S3Endpoint    string `name:"s3-endpoint" hidden:"true" env:"UR_S3_ENDPOINT"`
	S3Region      string `name:"s3-region" hidden:"true" env:"UR_S3_REGION"`
	S3Bucket      string `name:"s3-bucket" hidden:"true" env:"UR_S3_BUCKET"`
	S3AccessKeyID string `name:"s3-access-key-id" hidden:"true" env:"UR_S3_ACCESS_KEY_ID"`
	S3SecretKey   string `name:"s3-secret-key" hidden:"true" env:"UR_S3_SECRET_KEY"`
}

var (
	compilerRe         = regexp.MustCompile(`\(([A-Za-z0-9()., -]+) \w+-\w+(?:| android| default)\) ([\w@.-]+)`)
	knownDistributions = []distributionMatch{
		// Maps well known builders to the official distribution method that
		// they represent

		{regexp.MustCompile(`\steamcity@build\.syncthing\.net`), "GitHub"},
		{regexp.MustCompile(`\sjenkins@build\.syncthing\.net`), "GitHub"},
		{regexp.MustCompile(`\sbuilder@github\.syncthing\.net`), "GitHub"},

		{regexp.MustCompile(`\sdeb@build\.syncthing\.net`), "APT"},
		{regexp.MustCompile(`\sdebian@github\.syncthing\.net`), "APT"},

		{regexp.MustCompile(`\sdocker@syncthing\.net`), "Docker Hub"},
		{regexp.MustCompile(`\sdocker@build.syncthing\.net`), "Docker Hub"},
		{regexp.MustCompile(`\sdocker@github.syncthing\.net`), "Docker Hub"},

		{regexp.MustCompile(`\sandroid-builder@github\.syncthing\.net`), "Google Play"},
		{regexp.MustCompile(`\sandroid-.*teamcity@build\.syncthing\.net`), "Google Play"},

		{regexp.MustCompile(`\sandroid-.*vagrant@basebox-stretch64`), "F-Droid"},
		{regexp.MustCompile(`\svagrant@bullseye`), "F-Droid"},
		{regexp.MustCompile(`\svagrant@bookworm`), "F-Droid"},

		{regexp.MustCompile(`Anwender@NET2017`), "Syncthing-Fork (3rd party)"},

		{regexp.MustCompile(`\sbuilduser@(archlinux|svetlemodry)`), "Arch (3rd party)"},
		{regexp.MustCompile(`\ssyncthing@archlinux`), "Arch (3rd party)"},
		{regexp.MustCompile(`@debian`), "Debian (3rd party)"},
		{regexp.MustCompile(`@fedora`), "Fedora (3rd party)"},
		{regexp.MustCompile(`\sbrew@`), "Homebrew (3rd party)"},
		{regexp.MustCompile(`\sroot@buildkitsandbox`), "LinuxServer.io (3rd party)"},
		{regexp.MustCompile(`\sports@freebsd`), "FreeBSD (3rd party)"},
		{regexp.MustCompile(`\snix@nix`), "Nix (3rd party)"},
		{regexp.MustCompile(`.`), "Others"},
	}
)

type distributionMatch struct {
	matcher      *regexp.Regexp
	distribution string
}

func (cli *CLI) Run() error {
	slog.Info("starting", "version", build.Version)

	// Listening

	internalListener, err := net.Listen("tcp", cli.ListenInternal)
	if err != nil {
		slog.Error("listen", "error", err)
		return err
	}
	metricsListener, err := net.Listen("tcp", cli.Listen)
	if err != nil {
		slog.Error("listen", "error", err)
		return err
	}

	var geo *geoip.Provider
	if cli.GeoIPAccountID != 0 && cli.GeoIPLicenseKey != "" {
		geo, err = geoip.NewGeoLite2CityProvider(context.Background(), cli.GeoIPAccountID, cli.GeoIPLicenseKey, os.TempDir())
		if err != nil {
			slog.Error("geoip", "error", err)
			return err
		}
		go geo.Serve(context.TODO())
	}

	// s3

	var s3sess *s3.Session
	if cli.S3Endpoint != "" {
		s3sess, err = s3.NewSession(cli.S3Endpoint, cli.S3Region, cli.S3Bucket, cli.S3AccessKeyID, cli.S3SecretKey)
		if err != nil {
			slog.Error("s3", "error", err)
			return err
		}
	}

	if _, err := os.Stat(cli.DumpFile); err != nil && s3sess != nil {
		latestKey, err := s3sess.LatestKey()
		if err != nil {
			slog.Error("latest key", "error", err)
			goto resume
		}
		fd, err := os.Create(cli.DumpFile)
		if err != nil {
			slog.Error("creating dump file", "error", err)
			goto resume
		}
		if err := s3sess.Download(fd, latestKey); err != nil {
			slog.Error("downloading dump file", "error", err)
			_ = fd.Close()
			goto resume
		}
		if err := fd.Close(); err != nil {
			slog.Error("closing dump file", "error", err)
			goto resume
		}
		slog.Info("dump file downloaded", "key", latestKey)
	}
resume:

	// server

	srv := &server{
		geo:     geo,
		reports: xsync.NewMapOf[string, *contract.Report](),
	}

	if fd, err := os.Open(cli.DumpFile); err == nil {
		gr, err := gzip.NewReader(fd)
		if err == nil {
			srv.load(gr)
		} else {
			fd.Seek(0, 0)
			srv.load(fd) // XXX
		}
		fd.Close()
	}

	go func() {
		for range time.Tick(cli.DumpInterval) {
			fd, err := os.Create(cli.DumpFile + ".tmp")
			if err != nil {
				slog.Error("creating dump file", "error", err)
				continue
			}
			gw := gzip.NewWriter(fd)
			if err := srv.save(gw); err != nil {
				slog.Error("saving dump file", "error", err)
				continue
			}
			if err := gw.Close(); err != nil {
				slog.Error("closing gzip writer", "error", err)
				fd.Close()
				continue
			}
			if err := fd.Close(); err != nil {
				slog.Error("closing dump file", "error", err)
				continue
			}
			if err := os.Rename(cli.DumpFile+".tmp", cli.DumpFile); err != nil {
				slog.Error("renaming dump file", "error", err)
				continue
			}
			slog.Info("dump file saved")

			if s3sess != nil {
				key := fmt.Sprintf("reports-%s.jsons.gz", time.Now().UTC().Format("2006-01-02"))
				fd, err := os.Open(cli.DumpFile)
				if err != nil {
					slog.Error("opening dump file", "error", err)
					continue
				}
				if err := s3sess.Upload(fd, key); err != nil {
					slog.Error("uploading dump file", "error", err)
					continue
				}
				if err := fd.Close(); err != nil {
					slog.Error("closing dump file", "error", err)
					continue
				}
				slog.Info("dump file uploaded")
			}
		}
	}()

	// The internal metrics endpoint just serves metrics about what the
	// server is doing.

	http.Handle("/metrics", promhttp.Handler())

	internalSrv := http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
	}
	go internalSrv.Serve(internalListener)

	// New external metrics endpoint accepts reports from clients and serves
	// aggregated usage reporting metrics.

	ms := newMetricsSet(srv)
	reg := prometheus.NewRegistry()
	reg.MustRegister(ms)

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/newdata", srv.newDataHandler)

	metricsSrv := http.Server{
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 15 * time.Second,
		Handler:      mux,
	}
	return metricsSrv.Serve(metricsListener)
}

type server struct {
	geo     *geoip.Provider
	reports *xsync.MapOf[string, *contract.Report]
}

func (s *server) newDataHandler(w http.ResponseWriter, r *http.Request) {
	result := "fail"
	defer func() {
		// result is "accept" (new report), "replace" (existing report) or
		// "fail"
		metricReportsTotal.WithLabelValues(result).Inc()
	}()

	defer r.Body.Close()

	addr := r.Header.Get("X-Forwarded-For")
	if addr != "" {
		addr = strings.Split(addr, ", ")[0]
	} else {
		addr = r.RemoteAddr
	}

	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}

	log := slog.With("addr", addr)

	if net.ParseIP(addr) == nil {
		addr = ""
	}

	var rep contract.Report

	lr := &io.LimitedReader{R: r.Body, N: 40 * 1024}
	bs, _ := io.ReadAll(lr)
	if err := json.Unmarshal(bs, &rep); err != nil {
		log.Error("json decode", "error", err)
		http.Error(w, "JSON Decode Error", http.StatusInternalServerError)
		return
	}

	rep.Received = time.Now()
	rep.Date = rep.Received.UTC().Format("20060102")
	rep.Address = addr

	if err := rep.Validate(); err != nil {
		log.Error("validate", "error", err)
		http.Error(w, "Validation Error", http.StatusInternalServerError)
		return
	}

	if s.addReport(&rep) {
		result = "replace"
	} else {
		result = "accept"
	}
}

func (s *server) addReport(rep *contract.Report) bool {
	if s.geo != nil {
		if ip := net.ParseIP(rep.Address); ip != nil {
			if city, err := s.geo.City(ip); err == nil {
				rep.Country = city.Country.Names["en"]
				rep.City = city.City.Names["en"]
			}
		}
	}
	if rep.Country == "" {
		rep.Country = "Unknown"
	}
	if rep.City == "" {
		rep.City = "Unknown"
	}

	rep.Version = transformVersion(rep.Version)
	if strings.Contains(rep.Version, ".") {
		rep.MajorVersion = strings.Join(strings.SplitN(rep.Version, ".", 3)[:2], ".")
	}
	rep.OS, rep.Arch, _ = strings.Cut(rep.Platform, "-")

	if m := compilerRe.FindStringSubmatch(rep.LongVersion); len(m) == 3 {
		rep.Compiler = m[1]
		rep.Builder = m[2]
	}
	for _, d := range knownDistributions {
		if d.matcher.MatchString(rep.LongVersion) {
			rep.Distribution = d.distribution
			break
		}
	}

	_, loaded := s.reports.LoadAndStore(rep.UniqueID, rep)
	return loaded
}

func (s *server) save(w io.Writer) error {
	bw := bufio.NewWriter(w)
	enc := json.NewEncoder(bw)
	var err error
	s.reports.Range(func(k string, v *contract.Report) bool {
		err = enc.Encode(v)
		return err == nil
	})
	if err != nil {
		return err
	}
	return bw.Flush()
}

func (s *server) load(r io.Reader) {
	br := bufio.NewReader(r)
	dec := json.NewDecoder(br)
	for {
		var rep contract.Report
		if err := dec.Decode(&rep); err == io.EOF {
			break
		} else if err != nil {
			slog.Error("load", "error", err)
			break
		}
		s.reports.Store(rep.UniqueID, &rep)
	}
}

var (
	plusRe  = regexp.MustCompile(`(\+.*|[.-]dev\..*)$`)
	plusStr = "-dev"
)

// transformVersion returns a version number formatted correctly, with all
// development versions aggregated into one.
func transformVersion(v string) string {
	if v == "unknown-dev" {
		return v
	}
	if !strings.HasPrefix(v, "v") {
		v = "v" + v
	}
	v = plusRe.ReplaceAllString(v, plusStr)

	return v
}
