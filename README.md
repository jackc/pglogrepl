# pglogrepl

pglogrepl is a Go package for PostgreSQL logical replication.

## Example

In `example/pglogrepl_demo`, there is an example demo program that connects to a database and logs all messages sent over logical replication.

## Testing

Testing requires a user with replication permission, a database to replicate, access allowed in `pg_hba.conf`, and
logical replication enabled in `postgresql.conf`.

Create a database:

```
create database pglogrepl;
```

Create a user:

```
create user pglogrepl with replication password 'secret';
```

Add a replication line to your pg_hba.conf:

```
host replication pglogrepl 127.0.0.1/32 md5
```

Change the following settings in your postgresql.conf:

```
wal_level=logical
max_wal_senders=5
max_replication_slots=5
```

To run the tests set `PGLOGREPL_TEST_CONN_STRING` environment variable with a replication connection string (URL or DSN).

Example:

```
PGLOGREPL_TEST_CONN_STRING=postgres://pglogrepl:secret@127.0.0.1/pglogrepl?replication=database go test
```
