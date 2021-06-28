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
	"database/sql/driver"
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

type ReplicationMode int

const (
	LogicalReplication ReplicationMode = iota
	PhysicalReplication
)

// String formats the mode into a postgres valid string
func (mode ReplicationMode) String() string {
	if mode == LogicalReplication {
		return "LOGICAL"
	} else {
		return "PHYSICAL"
	}
}

// LSN is a PostgreSQL Log Sequence Number. See https://www.postgresql.org/docs/current/datatype-pg-lsn.html.
type LSN uint64

// String formats the LSN value into the XXX/XXX format which is the text format used by PostgreSQL.
func (lsn LSN) String() string {
	return fmt.Sprintf("%X/%X", uint32(lsn>>32), uint32(lsn))
}

func (lsn *LSN) decodeText(src string) error {
	lsnValue, err := ParseLSN(src)
	if err != nil {
		return err
	}
	*lsn = lsnValue

	return nil
}

// Scan implements the Scanner interface.
func (lsn *LSN) Scan(src interface{}) error {
	if lsn == nil {
		return nil
	}

	switch v := src.(type) {
	case uint64:
		*lsn = LSN(v)
	case string:
		if err := lsn.decodeText(v); err != nil {
			return err
		}
	case []byte:
		if err := lsn.decodeText(string(v)); err != nil {
			return err
		}
	default:
		return errors.Errorf("can not scan %T to LSN", src)
	}

	return nil
}

// Value implements the Valuer interface.
func (lsn LSN) Value() (driver.Value, error) {
	return driver.Value(lsn.String()), nil
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

// TimelineHistoryResult is the parsed result of the TIMELINE_HISTORY command.
type TimelineHistoryResult struct {
	FileName string
	Content  []byte
}

// TimelineHistory executes the TIMELINE_HISTORY command.
func TimelineHistory(ctx context.Context, conn *pgconn.PgConn, timeline int32) (TimelineHistoryResult, error) {
	sql := fmt.Sprintf("TIMELINE_HISTORY %d", timeline)
	return ParseTimelineHistory(conn.Exec(ctx, sql))
}

// ParseTimelineHistory parses the result of the TIMELINE_HISTORY command.
func ParseTimelineHistory(mrr *pgconn.MultiResultReader) (TimelineHistoryResult, error) {
	var thr TimelineHistoryResult
	results, err := mrr.ReadAll()
	if err != nil {
		return thr, err
	}

	if len(results) != 1 {
		return thr, errors.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) != 1 {
		return thr, errors.Errorf("expected 1 result row, got %d", len(result.Rows))
	}

	row := result.Rows[0]
	if len(row) != 2 {
		return thr, errors.Errorf("expected 2 result columns, got %d", len(row))
	}

	thr.FileName = string(row[0])
	thr.Content = row[1]
	return thr, nil
}

type CreateReplicationSlotOptions struct {
	Temporary      bool
	SnapshotAction string
	Mode           ReplicationMode
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
	sql := fmt.Sprintf("CREATE_REPLICATION_SLOT %s %s %s %s %s", slotName, temporaryString, options.Mode, outputPlugin, options.SnapshotAction)
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
	Timeline   int32 // 0 means current server timeline
	Mode       ReplicationMode
	PluginArgs []string
}

// StartReplication begins the replication process by executing the START_REPLICATION command.
func StartReplication(ctx context.Context, conn *pgconn.PgConn, slotName string, startLSN LSN, options StartReplicationOptions) error {
	var timelineString string
	if options.Timeline > 0 {
		timelineString = fmt.Sprintf("TIMELINE %d", options.Timeline)
		options.PluginArgs = append(options.PluginArgs, timelineString)
	}

	sql := fmt.Sprintf("START_REPLICATION SLOT %s %s %s ", slotName, options.Mode, startLSN)
	if options.Mode == LogicalReplication {
		if len(options.PluginArgs) > 0 {
			sql += fmt.Sprintf("(%s)", strings.Join(options.PluginArgs, ", "))
		}
	} else {
		sql += fmt.Sprintf("%s", timelineString)
	}

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

type BaseBackupOptions struct {
	// Request information required to generate a progress report, but might as such have a negative impact on the performance.
	Progress bool
	// Sets the label of the backup. If none is specified, a backup label of 'wal-g' will be used.
	Label string
	// Request a fast checkpoint.
	Fast bool
	// Include the necessary WAL segments in the backup. This will include all the files between start and stop backup in the pg_wal directory of the base directory tar file.
	WAL bool
	// By default, the backup will wait until the last required WAL segment has been archived, or emit a warning if log archiving is not enabled.
	// Specifying NOWAIT disables both the waiting and the warning, leaving the client responsible for ensuring the required log is available.
	NoWait bool
	// Limit (throttle) the maximum amount of data transferred from server to client per unit of time (kb/s).
	MaxRate int32
	// Include information about symbolic links present in the directory pg_tblspc in a file named tablespace_map.
	TablespaceMap bool
	// Disable checksums being verified during a base backup.
	// Note that NoVerifyChecksums=true is only supported since PG11
	NoVerifyChecksums bool
}

func (bbo BaseBackupOptions) sql() string {
	parts := []string{"BASE_BACKUP"}
	if bbo.Label != "" {
		parts = append(parts, "LABEL '"+strings.ReplaceAll(bbo.Label, "'", "''")+"'")
	}
	if bbo.Progress {
		parts = append(parts, "PROGRESS")
	}
	if bbo.Fast {
		parts = append(parts, "FAST")
	}
	if bbo.WAL {
		parts = append(parts, "WAL")
	}
	if bbo.NoWait {
		parts = append(parts, "NOWAIT")
	}
	if bbo.MaxRate >= 32 {
		parts = append(parts, fmt.Sprintf("MAX_RATE %d", bbo.MaxRate))
	}
	if bbo.TablespaceMap {
		parts = append(parts, "TABLESPACE_MAP")
	}
	if bbo.NoVerifyChecksums {
		parts = append(parts, "NOVERIFY_CHECKSUMS")
	}
	return strings.Join(parts, " ")
}

// BaseBackupTablespace represents a tablespace in the backup
type BaseBackupTablespace struct {
	OID      int32
	Location string
	Size     int8
}

// BaseBackupResult will hold the return values  of the BaseBackup command
type BaseBackupResult struct {
	LSN         LSN
	TimelineID  int32
	Tablespaces []BaseBackupTablespace
}

// StartBaseBackup begins the process for copying a basebackup by executing the BASE_BACKUP command.
func StartBaseBackup(ctx context.Context, conn *pgconn.PgConn, options BaseBackupOptions) (result BaseBackupResult, err error) {
	sql := options.sql()

	buf := (&pgproto3.Query{String: sql}).Encode(nil)
	err = conn.SendBytes(ctx, buf)
	if err != nil {
		return result, errors.Errorf("failed to send BASE_BACKUP: %w", err)
	}
	// From here Postgres returns result sets, but pgconn has no infrastructure to properly capture them.
	// So we capture data low level with sub functions, before we return from this function when we get to the CopyData part.
	result.LSN, result.TimelineID, err = getBaseBackupInfo(ctx, conn)
	if err != nil {
		return result, err
	}
	result.Tablespaces, err = getTableSpaceInfo(ctx, conn)
	return result, err
}

// getBaseBackupInfo returns the start or end position of the backup as returned by Postgres
func getBaseBackupInfo(ctx context.Context, conn *pgconn.PgConn) (start LSN, timelineID int32, err error) {
	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return start, timelineID, errors.Errorf("failed to receive message: %w", err)
		}
		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			if len(msg.Fields) != 2 {
				return start, timelineID, errors.Errorf("expected 2 column headers, received: %d", len(msg.Fields))
			}
			colName := string(msg.Fields[0].Name)
			if colName != "recptr" {
				return start, timelineID, errors.Errorf("unexpected col name for recptr col: %s", colName)
			}
			colName = string(msg.Fields[1].Name)
			if colName != "tli" {
				return start, timelineID, errors.Errorf("unexpected col name for tli col: %s", colName)
			}
		case *pgproto3.DataRow:
			if len(msg.Values) != 2 {
				return start, timelineID, errors.Errorf("expected 2 columns, received: %d", len(msg.Values))
			}
			colData := string(msg.Values[0])
			start, err = ParseLSN(colData)
			if err != nil {
				return start, timelineID, errors.Errorf("cannot convert result to LSN: %s", colData)
			}
			colData = string(msg.Values[1])
			tli, err := strconv.Atoi(colData)
			if err != nil {
				return start, timelineID, errors.Errorf("cannot convert timelineID to int: %s", colData)
			}
			timelineID = int32(tli)
		case *pgproto3.NoticeResponse:
		case *pgproto3.CommandComplete:
			return start, timelineID, nil
		default:
			return start, timelineID, errors.Errorf("unexpected response: %t", msg)
		}
	}
}

// getBaseBackupInfo returns the start or end position of the backup as returned by Postgres
func getTableSpaceInfo(ctx context.Context, conn *pgconn.PgConn) (tbss []BaseBackupTablespace, err error) {
	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return tbss, errors.Errorf("failed to receive message: %w", err)
		}
		switch msg := msg.(type) {
		case *pgproto3.RowDescription:
			if len(msg.Fields) != 3 {
				return tbss, errors.Errorf("expected 3 column headers, received: %d", len(msg.Fields))
			}
			colName := string(msg.Fields[0].Name)
			if colName != "spcoid" {
				return tbss, errors.Errorf("unexpected col name for spcoid col: %s", colName)
			}
			colName = string(msg.Fields[1].Name)
			if colName != "spclocation" {
				return tbss, errors.Errorf("unexpected col name for spclocation col: %s", colName)
			}
			colName = string(msg.Fields[2].Name)
			if colName != "size" {
				return tbss, errors.Errorf("unexpected col name for size col: %s", colName)
			}
		case *pgproto3.DataRow:
			if len(msg.Values) != 3 {
				return tbss, errors.Errorf("expected 3 columns, received: %d", len(msg.Values))
			}
			if msg.Values[0] == nil {
				continue
			}
			tbs := BaseBackupTablespace{}
			colData := string(msg.Values[0])
			OID, err := strconv.Atoi(colData)
			if err != nil {
				return tbss, errors.Errorf("cannot convert spcoid to int: %s", colData)
			}
			tbs.OID = int32(OID)
			tbs.Location = string(msg.Values[1])
			if msg.Values[2] != nil {
				colData := string(msg.Values[2])
				size, err := strconv.Atoi(colData)
				if err != nil {
					return tbss, errors.Errorf("cannot convert size to int: %s", colData)
				}
				tbs.Size = int8(size)
			}
			tbss = append(tbss, tbs)
		case *pgproto3.CommandComplete:
			return tbss, nil
		default:
			return tbss, errors.Errorf("unexpected response: %t", msg)
		}
	}
}

// NextTablespace consumes some msgs so we are at start of CopyData
func NextTableSpace(ctx context.Context, conn *pgconn.PgConn) (err error) {

	for {
		msg, err := conn.ReceiveMessage(ctx)
		if err != nil {
			return errors.Errorf("failed to receive message: %w", err)
		}

		switch msg := msg.(type) {
		case *pgproto3.CopyOutResponse:
			return nil
		case *pgproto3.CopyData:
			return nil
		case *pgproto3.ErrorResponse:
			return pgconn.ErrorResponseToPgError(msg)
		case *pgproto3.NoticeResponse:
		case *pgproto3.RowDescription:

		default:
			return errors.Errorf("unexpected response: %t", msg)
		}
	}
}

// FinishBaseBackup wraps up a backup after copying all results from the BASE_BACKUP command.
func FinishBaseBackup(ctx context.Context, conn *pgconn.PgConn) (result BaseBackupResult, err error) {

	// From here Postgres returns result sets, but pgconn has no infrastructure to properly capture them.
	// So we capture data low level with sub functions, before we return from this function when we get to the CopyData part.
	result.LSN, result.TimelineID, err = getBaseBackupInfo(ctx, conn)
	if err != nil {
		return result, err
	}
	result.Tablespaces, err = getTableSpaceInfo(ctx, conn)
	if err != nil {
		return result, err
	}
	_, err = SendStandbyCopyDone(context.Background(), conn)
	return result, nil
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

// CopyDoneResult is the parsed result as returned by the server after the client
// sends a CopyDone to the server to confirm ending the copy-both mode.
type CopyDoneResult struct {
	Timeline int32
	LSN      LSN
}

// SendStandbyCopyDone sends a StandbyCopyDone to the PostgreSQL server
// to confirm ending the copy-both mode.
func SendStandbyCopyDone(ctx context.Context, conn *pgconn.PgConn) (cdr *CopyDoneResult, err error) {
	cd := &pgproto3.CopyDone{}
	buf := cd.Encode(nil)
	err = conn.SendBytes(ctx, buf)
	if err != nil {
		return
	}
	mrr := conn.ReceiveResults(ctx)
	results, err := mrr.ReadAll()

	if len(results) != 2 {
		// Server returned a CopyDone, so client ended copy-both first.
		// Not at end of timeline, and server will not send a CopyDoneResult
		return cdr, errors.Errorf("expected 1 result set, got %d", len(results))
	}

	result := results[0]
	if len(result.Rows) > 1 {
		return cdr, errors.Errorf("expected 0 or 1 result row, got %d", len(result.Rows))
	}
	if len(result.Rows) == 0 {
		// This is expected behaviour when client was first to send CopyDone
		return
	}

	row := result.Rows[0]
	if len(row) != 2 {
		return cdr, errors.Errorf("expected 2 result columns, got %d", len(row))
	}

	timeline, err := strconv.Atoi(string(row[0]))
	if err != nil {
		return cdr, err
	}
	cdr = &CopyDoneResult{}
	cdr.Timeline = int32(timeline)
	cdr.LSN, err = ParseLSN(string(row[1]))
	return cdr, err
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
