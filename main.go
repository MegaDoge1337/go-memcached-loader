package main

import (
	"compress/gzip"
	"errors"
	"flag"
	"io"
	"log/slog"
	"megadoge/memcached_loader/pb"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/bradfitz/gomemcache/memcache"
	"google.golang.org/protobuf/proto"
)

const NORMAL_ERR_RATE float64 = 0.01

type AppsInstalled struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []uint32
}

func dotRename(path string) error {
	dir, name := filepath.Split(path)
	err := os.Rename(filepath.Join(dir, name), filepath.Join(dir, "."+name))
	if err != nil {
		return err
	}

	return nil
}

func parseAppInstalled(line string) AppsInstalled {
	fields := strings.Split(line, "\t")
	if len(fields) < 5 {
		return AppsInstalled{}
	}

	devType := fields[0]
	devId := fields[1]

	if devType == "" || devId == "" {
		return AppsInstalled{}
	}

	rawApps := strings.Split(fields[4], ",")
	var apps []uint32

	for _, rawApp := range rawApps {
		app, err := strconv.Atoi(rawApp)
		if err != nil {
			return AppsInstalled{}
		}
		apps = append(apps, uint32(app))
	}

	lat, err := strconv.ParseFloat(fields[2], 64)
	if err != nil {
		return AppsInstalled{}
	}

	lon, err := strconv.ParseFloat(fields[3], 64)
	if err != nil {
		return AppsInstalled{}
	}

	return AppsInstalled{
		devType: devType,
		devId:   devId,
		lat:     lat,
		lon:     lon,
		apps:    apps,
	}

}

func insertAppInstalled(mc *memcache.Client, key string, value []byte) (bool, error) {
	err := mc.Set(&memcache.Item{Key: key, Value: value})
	if err != nil {
		return false, err
	}
	return true, nil
}

func compareUserApps(a, b *pb.UserApps) bool {
	if a.GetLat() != b.GetLat() || a.GetLon() != b.GetLon() {
		return false
	}

	aApps := a.GetApps()
	bApps := b.GetApps()

	if len(aApps) != len(bApps) {
		return false
	}
	for i := range aApps {
		if aApps[i] != bApps[i] {
			return false
		}
	}

	return true
}

func protobufTest() (bool, error) {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
	rows := strings.Split(sample, "\n")
	for _, row := range rows {
		fields := strings.Split(row, "\t")

		lat, err := strconv.ParseFloat(fields[2], 64)
		if err != nil {
			return false, err
		}

		lon, err := strconv.ParseFloat(fields[3], 64)
		if err != nil {
			return false, err
		}

		rawApps := strings.Split(fields[4], ",")
		var apps []uint32

		for _, rawApp := range rawApps {
			app, err := strconv.Atoi(rawApp)
			if err != nil {
				return false, err
			}
			apps = append(apps, uint32(app))
		}

		ua := &pb.UserApps{
			Apps: apps,
			Lat:  &lat,
			Lon:  &lon,
		}

		packed, err := proto.Marshal(ua)
		if err != nil {
			return false, err
		}

		unpacked := &pb.UserApps{}
		err = proto.Unmarshal(packed, unpacked)
		if err != nil {
			return false, err
		}

		if !compareUserApps(ua, unpacked) {
			return false, errors.New("test failed, packed and unpacked messages not similar")
		}
	}

	return true, nil
}

func main() {
	jsonHandler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(jsonHandler)

	pattern := flag.String("pattern", "./data/appsinstalled/*.tsv.gz", "pattern for searching files")
	test := flag.Bool("test", false, "run protobuf-message test")
	dry := flag.Bool("dry", false, "run without sending to memcached")
	idfa := flag.String("idfa", "127.0.0.1:33013", "memcached server address")
	gaid := flag.String("gaid", "127.0.0.1:33014", "memcached server address")
	adid := flag.String("adid", "127.0.0.1:33015", "memcached server address")
	dvid := flag.String("dvid", "127.0.0.1:33016", "memcached server address")

	flag.Parse()

	logger.Info("Application started")
	logger.Debug("Current configuration", "pattern", *pattern, "test", *test, "dry", *dry, "idfa", *idfa, "gaid", *gaid, "adid", *adid, "dvid", *dvid)

	if *test {
		logger.Info("Run protobuf message test")
		_, err := protobufTest()
		if err != nil {
			logger.Error("Test failed", "error", err)
			os.Exit(1)
		}
		logger.Info("Test completed")
		os.Exit(0)
	}

	deviceMemc := make(map[string]*memcache.Client)
	deviceMemc["idfa"] = memcache.New(*idfa)
	deviceMemc["gaid"] = memcache.New(*gaid)
	deviceMemc["adid"] = memcache.New(*adid)
	deviceMemc["dvid"] = memcache.New(*dvid)

	entries, err := filepath.Glob(*pattern)
	if err != nil {
		logger.Error("No matches find for defined pattern", "pattern", pattern, "error", err)
		os.Exit(1)
	}

	for _, e := range entries {

		_, name := filepath.Split(e)
		if strings.HasPrefix(name, ".") {
			logger.Warn("Ignore processed file", "file", e)
			continue
		}

		processed := 0
		errors := 0

		fi, err := os.Open(e)
		if err != nil {
			logger.Error("Failed to open file", "file", e, "error", err)
			os.Exit(1)
		}
		defer fi.Close()

		fz, err := gzip.NewReader(fi)
		if err != nil {
			logger.Error("Failed to create gzip reader", "file", e, "error", err)
			os.Exit(1)
		}
		defer fz.Close()

		s, err := io.ReadAll(fz)
		if err != nil {
			logger.Error("Failed to read gzip content", "file", e, "error", err)
			os.Exit(1)
		}

		lines := strings.Split(string(s), "\n")

		for _, line := range lines {
			if len(line) == 0 {
				continue
			}

			appInstalled := parseAppInstalled(line)

			if appInstalled.devType == "" {
				logger.Error("Failed to parse line", "line", line)
				errors += 1
				continue
			}

			mc, exists := deviceMemc[appInstalled.devType]

			if !exists {
				logger.Error("Unknown device", "devType", appInstalled.devType)
				errors += 1
				continue
			}

			key := appInstalled.devType + ":" + appInstalled.devId

			ua := &pb.UserApps{
				Apps: appInstalled.apps,
				Lat:  &appInstalled.lat,
				Lon:  &appInstalled.lon,
			}

			packed, err := proto.Marshal(ua)
			if err != nil {
				logger.Error("Failed to marshal message", "message", ua, "error", err)
				errors += 1
				continue
			}

			if *dry {
				logger.Debug("Message processed", "dry", *dry, "message", packed)
				processed += 1
				continue
			}

			result, err := insertAppInstalled(mc, key, packed)

			if err != nil {
				logger.Error("Memcached insertion failed", "message", ua, "error", err)
				errors += 1
				continue
			}

			if result {
				logger.Debug("Message processed", "dry", *dry, "message", packed)
				processed += 1
			}
		}

		if processed == 0 {
			fz.Close()
			fi.Close()
			err := dotRename(e)
			if err != nil {
				logger.Error("Failed to rename file", "file", e)
			}
			continue
		}

		errorRate := float64(errors) / float64(processed)
		if errorRate < NORMAL_ERR_RATE {
			logger.Info("Acceptable error rate. Successfull load", "errorRate", errorRate)
		} else {
			logger.Error("High error rate. Failed load", "errorRate", errorRate, "normalErrorRate", NORMAL_ERR_RATE)
		}

		fz.Close()
		fi.Close()
		err = dotRename(e)
		if err != nil {
			logger.Error("Failed to rename file", "file", e)
		}
	}

	logger.Info("Application finished")
}
