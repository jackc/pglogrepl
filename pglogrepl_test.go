package pglogrepl_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
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
	assert.True(t, sysident.XlogPos > 0)
	assert.Greater(t, len(sysident.DBName), 0)
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
