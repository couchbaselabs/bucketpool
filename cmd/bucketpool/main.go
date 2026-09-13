// Command bucketpool manages Couchbase Server buckets for test suites that reuse them
// instead of dropping and recreating them between tests.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/couchbaselabs/bucketpool/internal/purge"
)

// Every setting reads from a flag or from an environment variable, and the flag wins.  The
// password is the exception: it has no flag, so that it stays out of the process list.
const (
	envConnectionString = "BUCKETPOOL_CONNECTION_STRING"
	envManagementURL    = "BUCKETPOOL_MANAGEMENT_URL"
	envUsername         = "BUCKETPOOL_USERNAME"
	envPassword         = "BUCKETPOOL_PASSWORD"
	envBucket           = "BUCKETPOOL_BUCKET"
	envTimeout          = "BUCKETPOOL_TIMEOUT"
)

const defaultTimeout = 120 * time.Second

const usage = `bucketpool manages Couchbase Server buckets for test suites that reuse them.

Usage:
    bucketpool <command> [flags]

Commands:
    purge    Empty a bucket in place, keeping its collections and indexes
    help     Show this message

Run "bucketpool <command> --help" for the flags of one command.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	switch command := os.Args[1]; command {
	case "purge":
		if err := runPurge(os.Args[2:]); err != nil {
			fmt.Fprintf(os.Stderr, "bucketpool: %v\n", err)
			os.Exit(1)
		}
	case "help", "-h", "--help":
		fmt.Print(usage)
	default:
		fmt.Fprintf(os.Stderr, "bucketpool: unknown command %q\n\n%s", command, usage)
		os.Exit(2)
	}
}

func runPurge(args []string) error {
	opts, timeout := parsePurgeFlags(args)

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	summary, purgeErr := purge.Run(ctx, opts)
	// A purge that never started has nothing to report, and printing a zero summary would
	// claim an empty bucket.
	if summary != nil {
		encoded, err := json.Marshal(summary)
		if err != nil {
			return err
		}
		fmt.Println(string(encoded))
	}
	return purgeErr
}

// parsePurgeFlags reads the purge settings from the flags, falling back to the environment.
// It exits the process if a required setting is missing, the way flag.ExitOnError does.
func parsePurgeFlags(args []string) (purge.Options, time.Duration) {
	fs := flag.NewFlagSet("purge", flag.ExitOnError)
	connectionString := fs.String("connection-string", os.Getenv(envConnectionString),
		"couchbase:// connection string ($"+envConnectionString+")")
	managementURL := fs.String("management-url", os.Getenv(envManagementURL),
		"http:// management URL, e.g. http://host:8091 ($"+envManagementURL+")")
	username := fs.String("username", os.Getenv(envUsername),
		"administrator username ($"+envUsername+")")
	bucket := fs.String("bucket", os.Getenv(envBucket),
		"bucket to empty ($"+envBucket+")")
	timeout := fs.Duration("timeout", defaultTimeout,
		"how long the feed and the purge together are allowed to take ($"+envTimeout+")")
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "Usage: bucketpool purge [flags]\n\n"+
			"Empty a bucket in place, keeping its scopes, collections and indexes.\n"+
			"Each flag falls back to the environment variable named beside it.\n"+
			"The administrator password is read from $%s, which has no flag so that it\n"+
			"stays out of the process list.\n\nFlags:\n", envPassword)
		fs.PrintDefaults()
	}
	// ExitOnError, so this only returns once the arguments parse.
	_ = fs.Parse(args)

	// The environment is read after parsing, so that an unusable value there cannot defeat a
	// -timeout flag that the caller passed explicitly.
	resolved, err := resolveTimeout(flagIsSet(fs, "timeout"), *timeout, os.Getenv(envTimeout), defaultTimeout)
	if err != nil {
		fmt.Fprintf(os.Stderr, "bucketpool: %v\n", err)
		os.Exit(2)
	}

	opts := purge.Options{
		ConnectionString: *connectionString,
		ManagementURL:    *managementURL,
		Username:         *username,
		Password:         os.Getenv(envPassword),
		Bucket:           *bucket,
	}

	var missing []string
	for name, value := range map[string]string{
		"-connection-string or $" + envConnectionString: opts.ConnectionString,
		"-management-url or $" + envManagementURL:       opts.ManagementURL,
		"-username or $" + envUsername:                  opts.Username,
		"$" + envPassword:                               opts.Password,
		"-bucket or $" + envBucket:                      opts.Bucket,
	} {
		if value == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		slices.Sort(missing)
		fmt.Fprintf(os.Stderr, "bucketpool: missing required settings: %s\n\n", strings.Join(missing, ", "))
		fs.Usage()
		os.Exit(2)
	}
	return opts, resolved
}

// flagIsSet reports whether the caller passed the named flag on the command line.
func flagIsSet(fs *flag.FlagSet, name string) bool {
	var set bool
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

// resolveTimeout picks the timeout to use.  The flag wins, and the environment variable is
// read, and checked, only when the flag is absent.  An unusable value there is an error the
// caller must see, rather than a silent default.
func resolveTimeout(flagSet bool, flagValue time.Duration, raw string, fallback time.Duration) (time.Duration, error) {
	if flagSet {
		return flagValue, nil
	}
	if raw == "" {
		return fallback, nil
	}
	parsed, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("$%s is not a duration: %w", envTimeout, err)
	}
	return parsed, nil
}
