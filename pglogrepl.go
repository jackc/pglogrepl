// pglogrepl package implements PostgreSQL logical replication client functionality.
//
// pglogrepl uses package github.com/jackc/pgconn as its underlying PostgreSQL connection.
// Use pgconn to establish a connection to PostgreSQL and then use the pglogrepl functions
// on that connection.
//
// Proper use of this package requires understanding the underlying PostgreSQL concepts.
// See https://www.postgresql.org/docs/current/protocol-replication.html.
package pglogrepl

import (
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgio"
	"github.com/jackc/pgproto3/v2"
	errors "golang.org/x/xerrors"
)

const (
	XLogDataByteID                = 'w'
	PrimaryKeepaliveMessageByteID = 'k'
	StandbyStatusUpdateByteID     = 'r'
)

// LSN is a PostgreSQL Log Sequence Number. See https://www.postgresql.org/docs/current/datatype-pg-lsn.html.
type LSN uint64

// String formats the LSN value into the XXX/XXX format which is the text format used by PostgreSQL.
func (lsn LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

// Parse the given XXX/XXX text format LSN used by PostgreSQL.
func ParseLSN(s string) (LSN, error) {
	var upperHalf uint64
	var lowerHalf uint64
	var nparsed int
	nparsed, err := fmt.Sscanf(s, "%X/%X", &upperHalf, &lowerHalf)
	if err != nil {
		return 0, errors.Errorf("failed to parse LSN: %w", err)
	}

	if nparsed != 2 {
		return 0, errors.Errorf("failed to parsed LSN: %s", s)
	}

	return LSN((upperHalf << 32) + lowerHalf), nil
}

// IdentifySystemResult is the parsed result of the IDENTIFY_SYSTEM command.
type IdentifySystemResult struct {
	SystemID string
	Timeline int32
	XLogPos  LSN
	DBName   string
}

// IdentifySystem executes the IDENTIFY_SYSTEM command.
func IdentifySystem(ctx context.Context, conn *pgconn.PgConn) (IdentifySystemResult, error) {
	return ParseIdentifySystem(conn.Exec(ctx, "IDENTIFY_SYSTEM"))
}

// ParseIdentifySystem parses the result of the IDENTIFY_SYSTEM command.
func ParseIdentifySystem(mrr *pgconn.MultiResultReader) (IdentifySystemResult, error) {
	var isr IdentifySystemResult
	results, err := mrr.ReadAll()
	if err != nil {
		return isr, err
	}

	if len(results) != 1 {
		return isr, errors.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return isr, errors.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 4 {
		return isr, errors.Errorf("expected 4 result columns, got %d", len(row))
	}

	isr.SystemID = string(row[0])
	timeline, err := strconv.ParseInt(string(row[1]), 10, 32)
	if err != nil {
		return isr, errors.Errorf("failed to parse timeline: %w", err)
	}
	isr.Timeline = int32(timeline)

	isr.XLogPos, err = ParseLSN(string(row[2]))
	if err != nil {
		return isr, errors.Errorf("failed to parse xlogpos as LSN: %w", err)
	}

	isr.DBName = string(row[3])

	return isr, nil
}

type CreateReplicationSlotOptions struct {
	Temporary      bool
	SnapshotAction string
}

// CreateReplicationSlotResult is the parsed results the CREATE_REPLICATION_SLOT command.
type CreateReplicationSlotResult struct {
	SlotName        string
	ConsistentPoint string
	SnapshotName    string
	OutputPlugin    string
}

// CreateReplicationSlot creates a logical replication slot.
func CreateReplicationSlot(
	ctx context.Context,
	conn *pgconn.PgConn,
	slotName string,
	outputPlugin string,
	options CreateReplicationSlotOptions,
) (CreateReplicationSlotResult, error) {
	var temporaryString string
	if options.Temporary {
		temporaryString = "TEMPORARY"
	}
	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s %s LOGICAL %s %s", slotName, temporaryString, outputPlugin, options.SnapshotAction)
	return ParseCreateReplicationSlot(conn.Exec(ctx, sql))
}

// ParseCreateReplicationSlot parses the result of the CREATE_REPLICATION_SLOT command.
func ParseCreateReplicationSlot(mrr *pgconn.MultiResultReader) (CreateReplicationSlotResult, error) {
	var crsr CreateReplicationSlotResult
	results, err := mrr.ReadAll()
	if err != nil {
		return crsr, err
	}

	if len(results) != 1 {
		return crsr, errors.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return crsr, errors.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 4 {
		return crsr, errors.Errorf("expected 4 result columns, got %d", len(row))
	}

	crsr.SlotName = string(row[0])
	crsr.ConsistentPoint = string(row[1])
	crsr.SnapshotName = string(row[2])
	crsr.OutputPlugin = string(row[3])

	return crsr, nil
}

type DropReplicationSlotOptions struct {
	Wait bool
}

// DropReplicationSlot drops a logical replication slot.
func DropReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName string, options DropReplicationSlotOptions) error {
	var waitString string
	if options.Wait {
		waitString = "WAIT"
	}
	sql := fmt.Sprintf("DROP_REPLICATION_SLOT %s %s", slotName, waitString)
	_, err := conn.Exec(ctx, sql).ReadAll()
	return err
}

type StartReplicationOptions struct {
	Timeline        int32 // 0 means current server timeline
	PluginArguments []string
}

// StartReplication begins the replication process by executing the START_REPLICATION command.
func StartReplication(ctx context.Context, conn *pgconn.PgConn, slotName string, startLSN LSN, options StartReplicationOptions) error {
	var timelineString string
	var pluginArgumentsString string
	if options.Timeline > 0 {
		timelineString = fmt.Sprintf("TIMELINE %d", options.Timeline)
	}
	if len(options.PluginArguments) > 0 {
		pluginArgumentsString = fmt.Sprintf("( %s )", strings.Join(options.PluginArguments, ", "))
	}
	sql := fmt.Sprintf("START_REPLICATION SLOT %s LOGICAL %s %s %s", slotName, startLSN, timelineString, pluginArgumentsString)

	buf := (&pgproto3.Query{String: sql}).Encode(nil)
	err := conn.SendBytes(ctx, buf)
	if err != nil {
		return errors.Errorf("failed to send START_REPLICATION: %w", err)
	}

	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return errors.Errorf("failed to receive message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.NoticeResponse:
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(msg)
		case *pgproto3.CopyBothResponse:
			// This signals the start of the replication stream.
			return nil
		default:
			return errors.Errorf("unexpected response: %t", msg)
		}
	}
}

type PrimaryKeepaliveMessage struct {
	ServerWALEnd   LSN
	ServerTime     time.Time
	ReplyRequested bool
}

// ParsePrimaryKeepaliveMessage parses a Primary keepalive message from the server.
func ParsePrimaryKeepaliveMessage(buf []byte) (PrimaryKeepaliveMessage, error) {
	var pkm PrimaryKeepaliveMessage
	if len(buf) != 17 {
		return pkm, errors.Errorf("PrimaryKeepaliveMessage must be 17 bytes, got %d", len(buf))
	}

	pkm.ServerWALEnd = LSN(binary.BigEndian.Uint64(buf))
	pkm.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(buf[8:])))
	pkm.ReplyRequested = buf[16] != 0

	return pkm, nil
}

type XLogData struct {
	WALStart     LSN
	ServerWALEnd LSN
	ServerTime   time.Time
	WALData      []byte
}

// ParseXLogData parses a XLogData message from the server.
func ParseXLogData(buf []byte) (XLogData, error) {
	var xld XLogData
	if len(buf) < 24 {
		return xld, errors.Errorf("XLogData must be at least 24 bytes, got %d", len(buf))
	}

	xld.WALStart = LSN(binary.BigEndian.Uint64(buf))
	xld.ServerWALEnd = LSN(binary.BigEndian.Uint64(buf[8:]))
	xld.ServerTime = pgTimeToTime(int64(binary.BigEndian.Uint64(buf[16:])))
	xld.WALData = buf[24:]

	return xld, nil
}

// StandbyStatusUpdate is a message sent from the client that acknowledges receipt of WAL records.
type StandbyStatusUpdate struct {
	WALWritePosition LSN       // The WAL position that's been locally written
	WALFlushPosition LSN       // The WAL position that's been locally flushed
	WALApplyPosition LSN       // The WAL position that's been locally applied
	ClientTime       time.Time // Client system clock time
	ReplyRequested   bool      // Request server to reply immediately.
}

// SendStandbyStatusUpdate sends a StandbyStatusUpdate to the PostgreSQL server.
//
// The only required field in ssu is WALWritePosition. If WALFlushPosition is 0 then WALWritePosition will be assigned
// to it. If WALApplyPosition is 0 then WALWritePosition will be assigned to it. If ClientTime is the zero value then
// the current time will be assigned to it.
func SendStandbyStatusUpdate(ctx context.Context, conn *pgconn.PgConn, ssu StandbyStatusUpdate) error {
	if ssu.WALFlushPosition == 0 {
		ssu.WALFlushPosition = ssu.WALWritePosition
	}
	if ssu.WALApplyPosition == 0 {
		ssu.WALApplyPosition = ssu.WALWritePosition
	}
	if ssu.ClientTime == (time.Time{}) {
		ssu.ClientTime = time.Now()
	}

	data := make([]byte, 0, 34)
	data = append(data, StandbyStatusUpdateByteID)
	data = pgio.AppendUint64(data, uint64(ssu.WALWritePosition))
	data = pgio.AppendUint64(data, uint64(ssu.WALFlushPosition))
	data = pgio.AppendUint64(data, uint64(ssu.WALApplyPosition))
	data = pgio.AppendInt64(data, timeToPgTime(ssu.ClientTime))
	if ssu.ReplyRequested {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}

	cd := &pgproto3.CopyData{Data: data}
	buf := cd.Encode(nil)

	return conn.SendBytes(ctx, buf)
}

const microsecFromUnixEpochToY2K = 946684800 * 1000000

func pgTimeToTime(microsecSinceY2K int64) time.Time {
	microsecSinceUnixEpoch := microsecFromUnixEpochToY2K + microsecSinceY2K
	return time.Unix(0, (microsecSinceUnixEpoch * 1000))
}

func timeToPgTime(t time.Time) int64 {
	microsecSinceUnixEpoch := t.Unix()*1000000 + int64(t.Nanosecond())/1000
	return microsecSinceUnixEpoch - microsecFromUnixEpochToY2K
}
