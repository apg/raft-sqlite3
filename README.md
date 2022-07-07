raft-sqlite3
============

This repository provides the `raftsqlite3` package. The package exports the
`Sqlite3Store` which is an implementation of both a `LogStore` and `StableStore`.

It is meant to be used as a backend for the `raft` [package
here](https://github.com/hashicorp/raft), and was heavily inspired by [raft-boltdb](https://github.com/hashicorp/raft-boltdb)

This implementation uses
[SQLlite3](https://sqlite.org), via the [go-sqlite3](https://github.com/mattn/go-sqlite3) package. This requires CGO to be enabled when compiling. Sorry.

## Metrics

The raft-sqlite3 library emits a number of metrics utilizing github.com/armon/go-metrics. Those metrics are detailed in the following table.

| Metric                              | Unit         | Type    | Description           |
| ----------------------------------- | ------------:| -------:|:--------------------- |
| `raft.boltdb.getLog`                | ms           | timer   | Measures the amount of time spent reading logs from the db. |
| `raft.boltdb.logBatchSize`          | bytes        | sample  | Measures the total size in bytes of logs being written to the db in a single batch. |
| `raft.boltdb.logsPerBatch`          | logs         | sample  | Measures the number of logs being written per batch to the db. |
| `raft.boltdb.logSize`               | bytes        | sample  | Measures the size of logs being written to the db. |
| `raft.boltdb.storeLogs`             | ms           | timer   | Measures the amount of time spent writing logs to the db. |
| `raft.boltdb.writeCapacity`         | logs/second  | sample  | Theoretical write capacity in terms of the number of logs that can be written per second. Each sample outputs what the capacity would be if future batched log write operations were similar to this one. This similarity encompasses 4 things: batch size, byte size, disk performance and boltdb performance. While none of these will be static and its highly likely individual samples of this metric will vary, aggregating this metric over a larger time window should provide a decent picture into how this BoltDB store can perform |
