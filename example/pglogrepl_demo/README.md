# pglogrepl_demo

`pglogrepl_demo` is a simple demo that connects to a database and logs all messages sent over logical replication. It
connects to the database specified by the environment variable `PGLOGREPL_DEMO_CONN_STRING`. The connection must have
replication privileges.

## Example Usage

First start `pglogrepl_demo`:

```
$ PGLOGREPL_DEMO_CONN_STRING="postgres://pglogrepl:secret@127.0.0.1/pglogrepl?replication=database" go run main.go
```

Start a `psql` connection in another terminal and run the following:

```
pglogrepl@127.0.0.1:5432 pglogrepl=# create table t (id int, name text);
CREATE TABLE
pglogrepl@127.0.0.1:5432 pglogrepl=# insert into t values(1, 'foo');
INSERT 0 1
pglogrepl@127.0.0.1:5432 pglogrepl=# update t set name='bar';
UPDATE 1
pglogrepl@127.0.0.1:5432 pglogrepl=# delete from t;
DELETE 1
```

You should see output like the following from the `pglogrepl_demo` process.

```
2019/08/22 20:04:35 SystemID: 6694401393180362549 Timeline: 1 XLogPos: 3/A667B740 DBName: pglogrepl
2019/08/22 20:04:35 Created temporary replication slot: pglogrepl_demo
2019/08/22 20:04:35 Logical replication started on slot pglogrepl_demo
2019/08/22 20:04:45 Sent Standby status message
2019/08/22 20:04:45 Primary Keepalive Message => ServerWALEnd: 3/A667B778 ServerTime: 2019-08-22 20:04:45.373665 -0500 CDT ReplyRequested: false
2019/08/22 20:04:51 XLogData => WALStart 3/A667B7A8 ServerWALEnd 3/A667B7A8 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData BEGIN 2435445
2019/08/22 20:04:51 XLogData => WALStart 3/A6693E30 ServerWALEnd 3/A6693E30 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData COMMIT 2435445
2019/08/22 20:04:55 Sent Standby status message
2019/08/22 20:04:55 Primary Keepalive Message => ServerWALEnd: 3/A6693E68 ServerTime: 2019-08-22 20:04:55.377208 -0500 CDT ReplyRequested: false
2019/08/22 20:04:59 XLogData => WALStart 3/A6693E68 ServerWALEnd 3/A6693E68 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData BEGIN 2435446
2019/08/22 20:04:59 XLogData => WALStart 3/A6693E68 ServerWALEnd 3/A6693E68 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData table public.t: INSERT: id[integer]:1 name[text]:'foo'
2019/08/22 20:04:59 XLogData => WALStart 3/A6693ED8 ServerWALEnd 3/A6693ED8 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData COMMIT 2435446
2019/08/22 20:05:04 XLogData => WALStart 3/A6693ED8 ServerWALEnd 3/A6693ED8 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData BEGIN 2435447
2019/08/22 20:05:04 XLogData => WALStart 3/A6693ED8 ServerWALEnd 3/A6693ED8 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData table public.t: UPDATE: id[integer]:1 name[text]:'bar'
2019/08/22 20:05:04 XLogData => WALStart 3/A6693F58 ServerWALEnd 3/A6693F58 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData COMMIT 2435447
2019/08/22 20:05:05 Sent Standby status message
2019/08/22 20:05:08 XLogData => WALStart 3/A6693F58 ServerWALEnd 3/A6693F58 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData BEGIN 2435448
2019/08/22 20:05:08 XLogData => WALStart 3/A6693F58 ServerWALEnd 3/A6693F58 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData table public.t: DELETE: (no-tuple-data)
2019/08/22 20:05:08 XLogData => WALStart 3/A6693FC0 ServerWALEnd 3/A6693FC0 ServerTime: 1999-12-31 18:00:00 -0600 CST WALData COMMIT 2435448
2019/08/22 20:05:10 Primary Keepalive Message => ServerWALEnd: 3/A6693FC0 ServerTime: 2019-08-22 20:05:10.148672 -0500 CDT ReplyRequested: false
2019/08/22 20:05:15 Sent Standby status message
2019/08/22 20:05:15 Primary Keepalive Message => ServerWALEnd: 3/A6693FF8 ServerTime: 2019-08-22 20:05:15.378933 -0500 CDT ReplyRequested: false
```
