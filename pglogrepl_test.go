package pglogrepl_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const slotName = "pglogrepl_test"
const outputPlugin = "test_decoding"

func closeConn(t testing.TB, conn *pgconn.PgConn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, conn.Close(ctx))
}

func TestIdentifySystem(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	assert.Greater(t, len(sysident.SystemID), 0)
	assert.True(t, sysident.Timeline > 0)
	assert.True(t, sysident.XLogPos > 0)
	assert.Greater(t, len(sysident.DBName), 0)
}

func TestGetHistoryFile(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING")+" replication=on")
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	tlh, err := pglogrepl.TimelineHistory(ctx, conn, 0)
	require.Error(t, err)

	tlh, err = pglogrepl.TimelineHistory(ctx, conn, 1)
	require.Error(t, err)

	if sysident.Timeline > 1 {
		// This test requires a Postgres with at least 1 timeline increase (promote, or recover)...
		tlh, err = pglogrepl.TimelineHistory(ctx, conn, sysident.Timeline)
		require.NoError(t, err)

		expectedFileName := fmt.Sprintf("%08X.history", sysident.Timeline)
		assert.Equal(t, expectedFileName, tlh.FileName)
		assert.Greater(t, len(tlh.Content), 0)
	}
}

func TestCreateReplicationSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	result, err := pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	assert.Equal(t, slotName, result.SlotName)
	assert.Equal(t, outputPlugin, result.OutputPlugin)
}

func TestDropReplicationSlot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	err = pglogrepl.DropReplicationSlot(ctx, conn, slotName, pglogrepl.DropReplicationSlotOptions{})
	require.NoError(t, err)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)
}

func TestStartReplication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Temporary: true})
	require.NoError(t, err)

	err = pglogrepl.StartReplication(ctx, conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		config, err := pgconn.ParseConfig(os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
		require.NoError(t, err)
		delete(config.RuntimeParams, "replication")

		conn, err := pgconn.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer closeConn(t, conn)

		_, err = conn.Exec(ctx, `
create table t(id int primary key, name text);

insert into t values (1, 'foo');
insert into t values (2, 'bar');
insert into t values (3, 'baz');

update t set name='quz' where id=3;

delete from t where id=2;

drop table t;
`).ReadAll()
		require.NoError(t, err)
	}()

	rxKeepAlive := func() pglogrepl.PrimaryKeepaliveMessage {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), cdMsg.Data[0])
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cdMsg.Data[1:])
		require.NoError(t, err)
		return pkm
	}

	rxXLogData := func() pglogrepl.XLogData {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(pglogrepl.XLogDataByteID), cdMsg.Data[0])
		xld, err := pglogrepl.ParseXLogData(cdMsg.Data[1:])
		require.NoError(t, err)
		return xld
	}

	rxKeepAlive()
	xld := rxXLogData()
	assert.Equal(t, "BEGIN", string(xld.WALData[:5]))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:1 name[text]:'foo'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:2 name[text]:'bar'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: INSERT: id[integer]:3 name[text]:'baz'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: UPDATE: id[integer]:3 name[text]:'quz'", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "table public.t: DELETE: id[integer]:2", string(xld.WALData))
	xld = rxXLogData()
	assert.Equal(t, "COMMIT", string(xld.WALData[:6]))
}

func TestStartReplicationPhysical(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*50)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, "", pglogrepl.CreateReplicationSlotOptions{Temporary: true, Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	err = pglogrepl.StartReplication(ctx, conn, slotName, sysident.XLogPos, pglogrepl.StartReplicationOptions{Mode: pglogrepl.PhysicalReplication})
	require.NoError(t, err)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		defer cancel()

		config, err := pgconn.ParseConfig(os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
		require.NoError(t, err)
		delete(config.RuntimeParams, "replication")

		conn, err := pgconn.ConnectConfig(ctx, config)
		require.NoError(t, err)
		defer closeConn(t, conn)

		_, err = conn.Exec(ctx, `
create table mytable(id int primary key, name text);
drop table mytable;
`).ReadAll()
		require.NoError(t, err)
	}()

	_ = func() pglogrepl.PrimaryKeepaliveMessage {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(pglogrepl.PrimaryKeepaliveMessageByteID), cdMsg.Data[0])
		pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(cdMsg.Data[1:])
		require.NoError(t, err)
		return pkm
	}

	rxXLogData := func() pglogrepl.XLogData {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)
		cdMsg, ok := msg.(*pgproto3.CopyData)
		require.True(t, ok)

		require.Equal(t, byte(pglogrepl.XLogDataByteID), cdMsg.Data[0])
		xld, err := pglogrepl.ParseXLogData(cdMsg.Data[1:])
		require.NoError(t, err)
		return xld
	}

	xld := rxXLogData()
	assert.Contains(t, string(xld.WALData), "mytable")

	copyDoneResult, err := pglogrepl.SendStandbyCopyDone(ctx, conn)
	require.NoError(t, err)
	assert.Nil(t, copyDoneResult)
}

func TestSendStandbyStatusUpdate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: sysident.XLogPos})
	require.NoError(t, err)
}
