# gocqlx Example

This is a simple example demonstrating how to use gocqlx with ScyllaDB or Apache Cassandra.

## Prerequisites

- Go 1.21 or later
- ScyllaDB or Apache Cassandra running locally on `127.0.0.1:9042` (default)
- Docker (optional, for easy setup)

## Quick Start with Docker

### Using Docker Compose (easiest)

```bash
# Start ScyllaDB
docker-compose up -d

# Run the example
go run main.go

# Clean up
docker-compose down
```

### Using Docker directly

```bash
# Start ScyllaDB
docker run --name scylla -p 9042:9042 -d scylladb/scylla

# Wait for ScyllaDB to start (about 30 seconds)
sleep 30

# Run the example
go run main.go

# Clean up
docker stop scylla
docker rm scylla
```

### Using Apache Cassandra

```bash
# Start Cassandra
docker run --name cassandra -p 9042:9042 -d cassandra:latest

# Wait for Cassandra to start (about 2 minutes)
sleep 120

# Run the example
go run main.go

# Clean up
docker stop cassandra
docker rm cassandra
```

## Manual Setup

If you prefer to run ScyllaDB or Cassandra manually:

1. Install and start ScyllaDB or Cassandra on your system
2. Ensure it's listening on `127.0.0.1:9042`
3. Run: `go run main.go`

This will:
- Create a keyspace named `example`
- Create a table `person`
- Insert a sample record
- Query and display the data

## What it does

The example shows basic gocqlx usage:
- Connecting to the cluster
- Executing DDL statements
- Using the query builder (`qb`) to build CQL queries
- Binding struct data to queries
- Scanning query results into structs

## Notes

- Uses `NetworkTopologyStrategy` with replication factor 1 for development compatibility
- The example is designed for single-node setups (development/testing)
- For production, adjust replication strategy based on your cluster topology

## Alternative: Suppressing Warnings

If you prefer to use `SimpleStrategy` and want to suppress the warnings, you can configure ScyllaDB/Cassandra:

```bash
# For ScyllaDB, add to scylla.yaml:
replication_strategy_warn_list: []
# or
replication_strategy_warn_list: ["NetworkTopologyStrategy", "EverywhereStrategy"]

# For Cassandra, add to cassandra.yaml:
replication_strategy_warn_list: []
```

Or modify the keyspace creation in the code:
```sql
CREATE KEYSPACE IF NOT EXISTS example WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
```

## Dependencies

- `github.com/gocql/gocql` - Cassandra/Scylla driver
- `github.com/scylladb/gocqlx/v3` - gocqlx library