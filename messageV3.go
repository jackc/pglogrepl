package pglogrepl

import (
	"time"
)

type MessageDecoderV3 interface {
	DecodeV3(src []byte, inStream bool) error
}

type BeginPrepareMessageV3 struct {
	baseMessage
	PrepareLSN        LSN
	TransactionEndLSN LSN
	// The time at which the transaction was prepared.
	PrepareTime time.Time
	// The transaction ID of the prepared transaction.
	Xid uint32
	// The user defined GID of the prepared transaction.
	Gid string
}

func (m *BeginPrepareMessageV3) DecodeV3(src []byte, _ bool) (err error) {
	if len(src) < 29 {
		return m.lengthError("BeginPrepareMessage", 29, len(src))
	}

	var low, used int
	m.PrepareLSN, used = m.decodeLSN(src)
	low += used
	m.TransactionEndLSN, used = m.decodeLSN(src[low:])
	low += used
	m.PrepareTime, used = m.decodeTime(src[low:])
	low += used
	m.Xid, used = m.decodeUint32(src[low:])
	low += used
	m.Gid, _ = m.decodeString(src[low:])
	m.SetType(MessageTypeBeginPrepare)

	return nil
}

type PrepareMessageV3 struct {
	baseMessage
	// Flags currently unused (must be 0).
	Flags             uint8
	PrepareLSN        LSN
	TransactionEndLSN LSN
	// The time at which the transaction was prepared.
	PrepareTime time.Time
	// The transaction ID of the prepared transaction.
	Xid uint32
	// The user defined GID of the prepared transaction.
	Gid string
}

func (m *PrepareMessageV3) DecodeV3(src []byte, _ bool) (err error) {
	if len(src) < 30 {
		return m.lengthError("PrepareMessage", 30, len(src))
	}

	var low, used int
	m.Flags = src[low]
	low += 1
	m.PrepareLSN, used = m.decodeLSN(src[low:])
	low += used
	m.TransactionEndLSN, used = m.decodeLSN(src[low:])
	low += used
	m.PrepareTime, used = m.decodeTime(src[low:])
	low += used
	m.Xid, used = m.decodeUint32(src[low:])
	low += used
	m.Gid, _ = m.decodeString(src[low:])
	m.SetType(MessageTypePrepare)

	return nil
}

type CommitPreparedMessageV3 struct {
	baseMessage
	// Flags currently unused (must be 0).
	Flags             uint8
	CommitLSN         LSN
	TransactionEndLSN LSN
	CommitTime        time.Time
	Xid               uint32
	// The user defined GID of the prepared transaction.
	Gid string
}

func (m *CommitPreparedMessageV3) DecodeV3(src []byte, _ bool) (err error) {
	if len(src) < 30 {
		return m.lengthError("CommitPreparedMessage", 30, len(src))
	}

	var low, used int
	m.Flags = src[low]
	low += 1
	m.CommitLSN, used = m.decodeLSN(src[low:])
	low += used
	m.TransactionEndLSN, used = m.decodeLSN(src[low:])
	low += used
	m.CommitTime, used = m.decodeTime(src[low:])
	low += used
	m.Xid, used = m.decodeUint32(src[low:])
	low += used
	m.Gid, _ = m.decodeString(src[low:])
	m.SetType(MessageTypeCommitPrepared)

	return nil
}

type RollbackPreparedMessageV3 struct {
	baseMessage
	// Flags currently unused (must be 0).
	Flags             uint8
	TransactionEndLSN LSN
	// The end LSN of the rollback of the prepared transaction.
	TransactionRollbackLSN LSN
	PrepareTime            time.Time
	RollbackTime           time.Time
	Xid                    uint32
	// The user defined GID of the prepared transaction.
	Gid string
}

func (m *RollbackPreparedMessageV3) DecodeV3(src []byte, _ bool) (err error) {
	if len(src) < 38 {
		return m.lengthError("RollbackPreparedMessage", 38, len(src))
	}

	var low, used int
	m.Flags = src[low]
	low += 1
	m.TransactionEndLSN, used = m.decodeLSN(src[low:])
	low += used
	m.TransactionRollbackLSN, used = m.decodeLSN(src[low:])
	low += used
	m.PrepareTime, used = m.decodeTime(src[low:])
	low += used
	m.RollbackTime, used = m.decodeTime(src[low:])
	low += used
	m.Xid, used = m.decodeUint32(src[low:])
	low += used
	m.Gid, _ = m.decodeString(src[low:])
	m.SetType(MessageTypeRollbackPrepared)

	return nil
}

type StreamPrepareMessageV3 struct {
	baseMessage
	// Flags currently unused (must be 0).
	Flags             uint8
	PrepareLSN        LSN
	TransactionEndLSN LSN
	PrepareTime       time.Time
	Xid               uint32
	// The user defined GID of the prepared transaction.
	Gid string
}

func (m *StreamPrepareMessageV3) DecodeV3(src []byte, _ bool) (err error) {
	if len(src) < 30 {
		return m.lengthError("StreamPrepareMessage", 30, len(src))
	}

	var low, used int
	m.Flags = src[low]
	low += 1
	m.PrepareLSN, used = m.decodeLSN(src[low:])
	low += used
	m.TransactionEndLSN, used = m.decodeLSN(src[low:])
	low += used
	m.PrepareTime, used = m.decodeTime(src[low:])
	low += used
	m.Xid, used = m.decodeUint32(src[low:])
	low += used
	m.Gid, _ = m.decodeString(src[low:])
	m.SetType(MessageTypeStreamPrepare)

	return nil
}

// ParseV2 parse a logical replication message from protocol version #3
// it accepts a slice of bytes read from PG and inStream parameter
// inStream must be true when StreamStartMessageV2 has been read
// it must be false after StreamStopMessageV2 has been read
func ParseV3(data []byte, inStream bool) (m Message, err error) {
	var decoder MessageDecoderV3
	msgType := MessageType(data[0])

	switch msgType {
	case MessageTypeBeginPrepare:
		decoder = new(BeginPrepareMessageV3)
	case MessageTypePrepare:
		decoder = new(PrepareMessageV3)
	case MessageTypeCommitPrepared:
		decoder = new(CommitPreparedMessageV3)
	case MessageTypeRollbackPrepared:
		decoder = new(RollbackPreparedMessageV3)
	case MessageTypeStreamPrepare:
		decoder = new(StreamPrepareMessageV3)
	default:
		// all messages from V2 are unchanged in V3
		// so we can just call ParseV2
		return ParseV2(data, inStream)
	}

	if err = decoder.DecodeV3(data[1:], inStream); err != nil {
		return nil, err
	}

	return decoder.(Message), nil
}
