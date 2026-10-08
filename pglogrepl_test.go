package pglogrepl_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func TestLSNSuite(t *testing.T) {
	suite.Run(t, new(lsnSuite))
}

type lsnSuite struct {
	suite.Suite
}

func (s *lsnSuite) R() *require.Assertions {
	return s.Require()
}

func (s *lsnSuite) Equal(e, a interface{}, args ...interface{}) {
	s.R().Equal(e, a, args...)
}

func (s *lsnSuite) NoError(err error) {
	s.R().NoError(err)
}

func (s *lsnSuite) TestScannerInterface() {
	var lsn pglogrepl.LSN
	lsnText := "16/B374D848"
	lsnUint64 := uint64(97500059720)
	var err error

	err = lsn.Scan(lsnText)
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	err = lsn.Scan([]byte(lsnText))
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	lsn = 0
	err = lsn.Scan(lsnUint64)
	s.NoError(err)
	s.Equal(lsnText, lsn.String())

	err = lsn.Scan(int64(lsnUint64))
	s.Error(err)
	s.T().Log(err)
}

func (s *lsnSuite) TestScanToNil() {
	var lsnPtr *pglogrepl.LSN
	err := lsnPtr.Scan("16/B374D848")
	s.NoError(err)
}

func (s *lsnSuite) TestValueInterface() {
	lsn := pglogrepl.LSN(97500059720)
	driverValue, err := lsn.Value()
	s.NoError(err)
	lsnStr, ok := driverValue.(string)
	s.R().True(ok)
	s.Equal("16/B374D848", lsnStr)
}

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

	config, err := pgconn.ParseConfig(os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	config.RuntimeParams["replication"] = "on"

	conn, err := pgconn.ConnectConfig(ctx, config)
	require.NoError(t, err)
	defer closeConn(t, conn)

	sysident, err := pglogrepl.IdentifySystem(ctx, conn)
	require.NoError(t, err)

	_, err = pglogrepl.TimelineHistory(ctx, conn, 0)
	require.Error(t, err)

	_, err = pglogrepl.TimelineHistory(ctx, conn, 1)
	require.Error(t, err)

	if sysident.Timeline > 1 {
		// This test requires a Postgres with at least 1 timeline increase (promote, or recover)...
		tlh, err := pglogrepl.TimelineHistory(ctx, conn, sysident.Timeline)
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
		var cdMsg *pgproto3.CopyData
		// Discard keepalive messages
		for {
			msg, err := conn.ReceiveMessage(ctx)
			require.NoError(t, err)
			var ok bool
			cdMsg, ok = msg.(*pgproto3.CopyData)
			require.True(t, ok)
			if cdMsg.Data[0] != pglogrepl.PrimaryKeepaliveMessageByteID {
				break
			}
		}
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

func TestBaseBackup(t *testing.T) {
	// base backup test could take a long time. Therefore it can be disabled.
	envSkipTest := os.Getenv("PGLOGREPL_SKIP_BASE_BACKUP")
	if envSkipTest != "" {
		skipTest, err := strconv.ParseBool(envSkipTest)
		if err != nil {
			t.Error(err)
		} else if skipTest {
			return
		}
	}

	conn, err := pgconn.Connect(context.Background(), os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	options := pglogrepl.BaseBackupOptions{
		NoVerifyChecksums: true,
		Progress:          true,
		Label:             "pglogrepltest",
		Fast:              true,
		WAL:               true,
		NoWait:            true,
		MaxRate:           1024,
		TablespaceMap:     true,
	}
	startRes, err := pglogrepl.StartBaseBackup(context.Background(), conn, options)
	require.NoError(t, err)
	require.GreaterOrEqual(t, startRes.TimelineID, int32(1))

	//Write the tablespaces
	for i := 0; i < len(startRes.Tablespaces)+1; i++ {
		f, err := os.CreateTemp("", fmt.Sprintf("pglogrepl_test_tbs_%d.tar", i))
		require.NoError(t, err)
		err = pglogrepl.NextTableSpace(context.Background(), conn)
		require.NoError(t, err)
		var message pgproto3.BackendMessage
	L:
		for {
			message, err = conn.ReceiveMessage(context.Background())
			require.NoError(t, err)
			switch msg := message.(type) {
			case *pgproto3.CopyData:
				_, err := f.Write(msg.Data)
				require.NoError(t, err)
			case *pgproto3.CopyDone:
				break L
			default:
				t.Errorf("Received unexpected message: %#v\n", msg)
			}
		}
		err = f.Close()
		require.NoError(t, err)
	}

	stopRes, err := pglogrepl.FinishBaseBackup(context.Background(), conn)
	require.NoError(t, err)
	require.Equal(t, startRes.TimelineID, stopRes.TimelineID)
	require.Equal(t, len(stopRes.Tablespaces), 0)
	require.Less(t, uint64(startRes.LSN), uint64(stopRes.LSN))
	require.Greater(t, startRes.PgDataSize, 0)
	_, err = pglogrepl.StartBaseBackup(context.Background(), conn, options)
	require.NoError(t, err)
}

func TestBaseBackupManifest(t *testing.T) {
	// base backup test could take a long time. Therefore it can be disabled.
	envSkipTest := os.Getenv("PGLOGREPL_SKIP_BASE_BACKUP")
	if envSkipTest != "" {
		skipTest, err := strconv.ParseBool(envSkipTest)
		require.NoError(t, err)
		if skipTest {
			t.Skip("PGLOGREPL_SKIP_BASE_BACKUP=true, skipping base backup test")
		}
	}

	// Use timeout so the test cannot hang forever.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	serverVersion, err := serverMajorVersion(conn)
	require.NoError(t, err)
	if serverVersion < 15 {
		t.Skip("backup manifest framing via CopyData subtypes is PG15+; PG13/14 stream raw tar")
	}

	manifestData, filesWritten := streamBB(ctx, t, conn, false)
	require.Greater(t, len(manifestData), 1)
	require.GreaterOrEqual(t, len(filesWritten), 2) // manifest + base
}

func TestBaseBackupIncremental(t *testing.T) {
	// base backup test could take a long time. Therefore it can be disabled.
	envSkipTest := os.Getenv("PGLOGREPL_SKIP_BASE_BACKUP")
	if envSkipTest != "" {
		skipTest, err := strconv.ParseBool(envSkipTest)
		require.NoError(t, err)
		if skipTest {
			t.Skip("PGLOGREPL_SKIP_BASE_BACKUP=true, skipping base backup test")
		}
	}

	// Use timeout so the test cannot hang forever.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgconn.Connect(ctx, os.Getenv("PGLOGREPL_TEST_CONN_STRING"))
	require.NoError(t, err)
	defer closeConn(t, conn)

	// skip when major version < 17
	serverVersion, err := serverMajorVersion(conn)
	require.NoError(t, err)
	if serverVersion < 17 {
		t.Skip()
	}

	// incremental requires WAL summarization
	requireSummarizeWalOn(ctx, t, conn)

	// create basebackup
	manifestData, filesWritten := streamBB(ctx, t, conn, false)
	require.Greater(t, len(manifestData), 1)
	require.GreaterOrEqual(t, len(filesWritten), 2) // manifest + base
	manifestRdr := io.NopCloser(bytes.NewReader([]byte(manifestData)))

	for _, f := range filesWritten {
		t.Logf("base. written file: %s\n", f)
	}

	// create incremental backup
	// 1) upload manifest
	err = pglogrepl.UploadManifest(ctx, conn, manifestRdr)
	require.NoError(t, err)
	// 2) streaming incremental backup
	manifestDataIncremental, filesWritten := streamBB(ctx, t, conn, true)
	require.Greater(t, len(manifestDataIncremental), 1)
	require.GreaterOrEqual(t, len(filesWritten), 2) // manifest + base

	for _, f := range filesWritten {
		t.Logf("incremental. written file: %s\n", f)
	}
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

// Helpers

//nolint:gocritic
func readCString(buf []byte) (string, []byte, error) {
	idx := bytes.IndexByte(buf, 0)
	if idx < 0 {
		return "", nil, fmt.Errorf("invalid CString: %q", string(buf))
	}
	return string(buf[:idx]), buf[idx+1:], nil
}

// Wrap any io.Writer to give it a no-op Close() so it satisfies writeCloser.
type nopCloser struct {
	io.Writer
}

// Generalized writer interface for "current stream target".
type writeCloser interface {
	io.Writer
	io.Closer
}

func (n nopCloser) Close() error {
	return nil
}

func streamBB(ctx context.Context, t *testing.T, conn *pgconn.PgConn, incremental bool) (string, []string) {
	t.Helper()

	var (
		curTarget     writeCloser
		curTargetName string
		manifestBuf   bytes.Buffer
		filesWritten  []string
	)

	_, err := pglogrepl.StartBaseBackup(ctx, conn, pglogrepl.BaseBackupOptions{
		Label:         "pglogrepltest",
		Progress:      false,
		Fast:          true,
		WAL:           false,
		NoWait:        true,
		MaxRate:       0,
		TablespaceMap: true,
		Manifest:      true,
		Incremental:   incremental,
	})
	require.NoError(t, err)

	closeCurrent := func() {
		if curTarget == nil {
			return
		}
		if len(curTargetName) > 0 {
			filesWritten = append(filesWritten, curTargetName)
		}
		require.NoError(t, curTarget.Close())
		curTarget = nil
		curTargetName = ""
	}

	for {
		msg, err := conn.ReceiveMessage(ctx)
		require.NoError(t, err)

		switch m := msg.(type) {
		case *pgproto3.CopyOutResponse:
			// nothing interesting here
			continue

		case *pgproto3.CopyData:
			if len(m.Data) == 0 {
				t.Logf("skip empty CopyData")
				continue
			}
			switch m.Data[0] {
			case 'n':
				// New file header (tar member)
				closeCurrent()

				filename, rest, err := readCString(m.Data[1:])
				require.NoError(t, err)

				tsPath, _, err := readCString(rest)
				require.NoError(t, err)

				if !strings.Contains(filename, "base") {
					assert.Greater(t, len(tsPath), 1)
				}

				bbTar := strings.TrimPrefix(filename, "./")

				// Still write backup files to temp, but we don't care about contents in this test.
				f, err := os.CreateTemp("", "*-"+bbTar)
				require.NoError(t, err)
				curTarget = f
				curTargetName = filepath.ToSlash(f.Name())

			case 'd':
				// File or manifest data
				require.NotNil(t, curTarget, "received data but no active writer")

				_, err := curTarget.Write(m.Data[1:])
				require.NoError(t, err)

			case 'm':
				// Switch to manifest stream -> write into buffer instead of a file.
				closeCurrent()
				manifestBuf.Reset()
				curTarget = nopCloser{Writer: &manifestBuf}

			case 'p':
				// only if Progress: true (we disabled Progress above)

			default:
				// unexpected data type – fail fast so we don't spin forever
				t.Logf("skip unexpected CopyData subtype: %q (len=%d)", m.Data[0], len(m.Data))
				continue
			}

		case *pgproto3.CopyDone:
			// backup stream complete
			closeCurrent()

			_, err := pglogrepl.FinishBaseBackup(ctx, conn)
			require.NoError(t, err)

			// assert manifest is meaningful

			// 1) non-empty
			manStr := strings.TrimSpace(manifestBuf.String())
			require.NotEmpty(t, manStr, "manifest should not be empty")

			// save for inspecting
			f, err := os.CreateTemp(os.TempDir(), "manifest")
			require.NoError(t, err)
			_, err = f.Write(manifestBuf.Bytes())
			require.NoError(t, err)
			filesWritten = append(filesWritten, filepath.ToSlash(f.Name()))

			// 2) valid json with some keys
			var manifestJSON map[string]any
			err = json.Unmarshal(manifestBuf.Bytes(), &manifestJSON)
			require.NoError(t, err, "manifest must be valid JSON")
			require.NotEmpty(t, manifestJSON, "manifest JSON must have at least one key")

			// 3) expect keys
			_, hasVersion := manifestJSON["PostgreSQL-Backup-Manifest-Version"]
			assert.True(t, hasVersion, "manifest should contain 'PostgreSQL-Backup-Manifest-Version' field")

			// 4) check incremental
			if incremental {
				assert.True(t, strings.Contains(manStr, "INCREMENTAL"))
			}

			return manifestBuf.String(), filesWritten

		default:
			// For this test, any other message is unexpected; better to fail than hang.
			t.Fatalf("unexpected message type: %T", msg)
		}
	}
}

func serverMajorVersion(conn *pgconn.PgConn) (int, error) {
	verString := conn.ParameterStatus("server_version")
	dot := strings.IndexByte(verString, '.')
	if dot == -1 {
		return 0, fmt.Errorf("bad server version string: '%s'", verString)
	}
	return strconv.Atoi(verString[:dot])
}

func requireSummarizeWalOn(ctx context.Context, t *testing.T, conn *pgconn.PgConn) {
	t.Helper()

	rr := conn.Exec(ctx, "SHOW summarize_wal")
	res, err := rr.ReadAll()
	require.NoError(t, err)
	require.NotEmpty(t, res)
	require.Greater(t, len(res[0].Rows), 0)

	val := strings.TrimSpace(string(res[0].Rows[0][0])) // first row, first column
	if val != "on" {
		t.Skipf("summarize_wal=%q; start postgres with -c summarize_wal=on (required for incremental base backups)", val)
	}
}
