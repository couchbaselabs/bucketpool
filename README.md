# bucketpool

`bucketpool` empties a Couchbase Server bucket in place, for integration tests that reuse a
bucket instead of dropping it and creating it again. The bucket, its scopes, its collections
and its indexes stay exactly as they are.

Inspired by the in-process bucket pool in <https://github.com/couchbase/sync_gateway>, this is
used by <https://github.com/couchbaselabs/couchbase-lite-tests> for mobile testing.

Sync Gateway does not care about:

1. tombstones with no xattrs
1. extra collections on a bucket. A Sync Gateway database registers the ones it uses.
1. the presence or absence of indexes. A Sync Gateway database creates the ones it needs, so
   an index that survives the purge only saves the time of creating it again.

## Install

```
go install github.com/couchbaselabs/bucketpool/cmd/bucketpool@latest
```

You can also run it without installing it:

```
go run github.com/couchbaselabs/bucketpool/cmd/bucketpool@latest help
```

## Commands

| Command | What it does                                                             |
| ------- | ------------------------------------------------------------------------ |
| `purge` | Empties a bucket in place, and keeps its scopes, collections and indexes |
| `help`  | Shows the command list                                                   |

## purge

```
BUCKETPOOL_PASSWORD=password bucketpool purge \
    --connection-string couchbase://localhost \
    --management-url http://localhost:8091 \
    --username Administrator \
    --bucket data-bucket
```

Every setting takes a flag or an environment variable, and the flag wins. The same run
through the environment alone:

```
export BUCKETPOOL_CONNECTION_STRING=couchbase://localhost
export BUCKETPOOL_MANAGEMENT_URL=http://localhost:8091
export BUCKETPOOL_USERNAME=Administrator
export BUCKETPOOL_PASSWORD=password
export BUCKETPOOL_BUCKET=data-bucket
bucketpool purge
```

| Flag                  | Environment variable           | Required | Meaning                                                                       |
| --------------------- | ------------------------------ | -------- | ----------------------------------------------------------------------------- |
| `--connection-string` | `BUCKETPOOL_CONNECTION_STRING` | yes      | The `couchbase://` connection string of the cluster                           |
| `--management-url`    | `BUCKETPOOL_MANAGEMENT_URL`    | yes      | The `http://` management URL of one node, for example `http://host:8091`      |
| `--username`          | `BUCKETPOOL_USERNAME`          | yes      | The administrator username                                                    |
| _(none)_              | `BUCKETPOOL_PASSWORD`          | yes      | The administrator password                                                    |
| `--bucket`            | `BUCKETPOOL_BUCKET`            | yes      | The bucket to empty                                                           |
| `--timeout`           | `BUCKETPOOL_TIMEOUT`           | no       | How long the feed and the purge together are allowed to take (default `30m`)  |
| `--concurrency`       | `BUCKETPOOL_CONCURRENCY`       | no       | How many documents to purge at once (default `1024`)                          |

The password has no flag, so that it stays out of the process list.

The purge covers every collection in the bucket, the `_system` scope included. Creating an
index writes a document into `_system._query`, and the purge removes it.

On success the command prints one JSON line with the number of documents it saw and the
number it purged:

```
{"processed":128,"purged":128}
```

If the purge never starts, the command prints nothing on standard output and exits
non-zero. If it starts and some documents fail, it prints the summary, reports the
failures on standard error, and still exits non-zero. The report names at most ten
documents, and gives the total count when more than ten failed.

Purging one document costs one or two network round trips, so the distance to the cluster,
not the bandwidth, sets the rate. Raise `--concurrency` when the round trip is the limit.
The gain flattens out below 512 against a single node on localhost, and the request queue of
the client holds 2048 requests per node.

## Develop

```
go build ./...
go vet ./...
go test ./...
```

The tests cover the parts that need no cluster: xattr parsing, the feed observer, the
sub-document batching, the failure report, and the flag handling. Everything else needs a
live Couchbase Server.
