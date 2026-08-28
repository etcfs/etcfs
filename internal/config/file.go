// Package config — configuration file and environment layering.
//
// The daemon takes the same settings from three places, in decreasing
// precedence: a command-line flag, an environment variable, a YAML file.
// Anything none of them sets keeps the flag's own default.
//
// There is deliberately only one vocabulary: a YAML key and an environment
// variable are named after the flag they stand in for, so `--metrics-addr`
// is `metrics-addr:` in the file and `ETCFS_METRICS_ADDR` in the
// environment. Nothing has to be looked up in a translation table, and a
// key that matches no flag is an error rather than a setting that silently
// does nothing.
package config

import (
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// DefaultConfigFile is read when neither --config nor ETCFS_CONFIG names a
// file. Its absence is not an error — a cluster that passes every setting on
// the command line, as this repo's own scripts do, never creates one. A file
// named explicitly and then missing is an error, since the operator asked for
// it by name.
const DefaultConfigFile = "/etc/etcfs/etcfuse-meta.yaml"

// modeFlags select what the process does rather than how it is configured, so
// they are answered only by the command line. A file or an environment
// variable that could turn on --fsck would leave a node that never starts its
// daemon at all, with nothing on the command line to explain why.
var modeFlags = map[string]bool{
	"config":  true,
	"version": true,
	"fsck":    true,
	"info":    true,
}

// envName is the environment variable a flag answers to.
func envName(flagName string) string {
	return "ETCFS_" + strings.ToUpper(strings.ReplaceAll(flagName, "-", "_"))
}

// wasSet reports whether the command line set a flag, as opposed to it still
// holding its default.
func wasSet(set *flag.FlagSet, name string) bool {
	found := false
	set.Visit(func(f *flag.Flag) {
		if f.Name == name {
			found = true
		}
	})
	return found
}

// configPath resolves which file to read and whether it was asked for by name.
func configPath(set *flag.FlagSet, flagValue string) (path string, explicit bool) {
	if wasSet(set, "config") {
		return flagValue, true
	}
	if v, ok := os.LookupEnv(envName("config")); ok {
		return v, true
	}
	return DefaultConfigFile, false
}

// loadFile reads and parses the YAML configuration file.
func loadFile(path string, explicit bool) (map[string]any, error) {
	data, err := os.ReadFile(path)
	switch {
	case errors.Is(err, fs.ErrNotExist) && !explicit:
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read config file: %w", err)
	}
	var m map[string]any
	if err := yaml.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	return m, nil
}

// yamlValue renders a YAML scalar as the string its flag knows how to parse.
//
// A sequence becomes the comma-separated form the flag already accepts, so
// --etcd-endpoints can be written as a YAML list without the flag needing a
// second parser for it.
func yamlValue(key string, v any) (string, error) {
	switch t := v.(type) {
	case nil:
		return "", nil
	case []any:
		parts := make([]string, 0, len(t))
		for _, e := range t {
			s, err := yamlValue(key, e)
			if err != nil {
				return "", err
			}
			parts = append(parts, s)
		}
		return strings.Join(parts, ","), nil
	case map[string]any:
		return "", fmt.Errorf("config key %q: want a scalar or a list, got a mapping", key)
	default:
		return fmt.Sprintf("%v", t), nil
	}
}

// applyLayers fills in every flag the command line left alone, from the
// environment first and the file second.
func applyLayers(set *flag.FlagSet, file map[string]any) error {
	known := map[string]bool{}
	set.VisitAll(func(f *flag.Flag) { known[f.Name] = true })
	for key := range file {
		if modeFlags[key] {
			return fmt.Errorf("config key %q selects what the process does and is accepted only on the command line", key)
		}
		if !known[key] {
			return fmt.Errorf("unknown config key %q", key)
		}
	}

	var firstErr error
	set.VisitAll(func(f *flag.Flag) {
		if firstErr != nil || modeFlags[f.Name] || wasSet(set, f.Name) {
			return
		}
		value, ok := os.LookupEnv(envName(f.Name))
		source := envName(f.Name)
		if !ok {
			raw, present := file[f.Name]
			if !present {
				return
			}
			source = "config key " + f.Name
			if value, firstErr = yamlValue(f.Name, raw); firstErr != nil {
				return
			}
		}
		if err := set.Set(f.Name, value); err != nil {
			firstErr = fmt.Errorf("%s: %w", source, err)
		}
	})
	return firstErr
}

// layer applies the environment and the configuration file underneath the
// flags already parsed into set.
func layer(set *flag.FlagSet, configFlag string) error {
	path, explicit := configPath(set, configFlag)
	file, err := loadFile(path, explicit)
	if err != nil {
		return err
	}
	return applyLayers(set, file)
}
