package pglogrepl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestTooShortMessageV3Suite(t *testing.T) {
	suite.Run(t, new(tooShortMessageV3Suite))
}

type tooShortMessageV3Suite struct {
	messageSuite
}

func (s *tooShortMessageV3Suite) TestTooShortError() {
	msg := make([]byte, 29)

	tooShortForMessageType := func(messageType MessageType,
		messageTypeName string, minBytes int) {
		for _, inStream := range []bool{false, true} {
			msg[0] = uint8(messageType)
			m, err := ParseV3(msg, inStream)
			s.Nil(m)
			s.ErrorContains(err,
				fmt.Sprintf("%s must have %d bytes, got 28 bytes", messageTypeName, minBytes))
		}
	}

	tooShortForMessageType(MessageTypeBeginPrepare, "BeginPrepareMessage", 29)
	tooShortForMessageType(MessageTypePrepare, "PrepareMessage", 30)
	tooShortForMessageType(MessageTypeCommitPrepared, "CommitPreparedMessage", 30)
	tooShortForMessageType(MessageTypeRollbackPrepared, "RollbackPreparedMessage", 38)
	tooShortForMessageType(MessageTypeStreamPrepare, "StreamPrepareMessage", 30)
}

func TestBeginPrepareMessageV3Suite(t *testing.T) {
	suite.Run(t, new(beginPrepareMessageV3Suite))
}

type beginPrepareMessageV3Suite struct {
	messageSuite
}

func (s *beginPrepareMessageV3Suite) Test() {
	// last byte is for NUL terminator
	msg := make([]byte, 1+8+8+8+4+4+1)
	msg[0] = uint8(MessageTypeBeginPrepare)
	prepareLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	xid := s.newXid()
	gid := "test"
	bigEndian.PutUint64(msg[1:], uint64(prepareLSN))
	bigEndian.PutUint64(msg[1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+8+8:], prepareTimeU64)
	bigEndian.PutUint32(msg[1+8+8+8:], xid)
	s.putString(msg[1+8+8+8+4:], gid)

	expected := &BeginPrepareMessageV3{
		PrepareLSN:        prepareLSN,
		TransactionEndLSN: transactionEndLSN,
		PrepareTime:       prepareTime,
		Xid:               xid,
		Gid:               gid,
	}
	expected.msgType = MessageTypeBeginPrepare
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	// ideally we should error if inStream true
	// but sticking to what other messages do for now
	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*BeginPrepareMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func (s *beginPrepareMessageV3Suite) TestNoGID() {
	msg := make([]byte, 1+8+8+8+4+1)
	msg[0] = uint8(MessageTypeBeginPrepare)
	prepareLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	xid := s.newXid()
	bigEndian.PutUint64(msg[1:], uint64(prepareLSN))
	bigEndian.PutUint64(msg[1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+8+8:], prepareTimeU64)
	bigEndian.PutUint32(msg[1+8+8+8:], xid)
	msg[1+8+8+8+4] = 0

	expected := &BeginPrepareMessageV3{
		PrepareLSN:        prepareLSN,
		TransactionEndLSN: transactionEndLSN,
		PrepareTime:       prepareTime,
		Xid:               xid,
	}
	expected.msgType = MessageTypeBeginPrepare
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*BeginPrepareMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func TestPrepareMessageV3Suite(t *testing.T) {
	suite.Run(t, new(prepareMessageV3Suite))
}

type prepareMessageV3Suite struct {
	messageSuite
}

func (s *prepareMessageV3Suite) Test() {
	msg := make([]byte, 1+1+8+8+8+4+4+1)
	msg[0] = uint8(MessageTypePrepare)
	msg[1] = 0
	prepareLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	xid := s.newXid()
	gid := "test"
	bigEndian.PutUint64(msg[1+1:], uint64(prepareLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], prepareTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8:], xid)
	s.putString(msg[1+1+8+8+8+4:], gid)

	expected := &PrepareMessageV3{
		Flags:             0,
		PrepareLSN:        prepareLSN,
		TransactionEndLSN: transactionEndLSN,
		PrepareTime:       prepareTime,
		Xid:               xid,
		Gid:               gid,
	}
	expected.msgType = MessageTypePrepare
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*PrepareMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func (s *prepareMessageV3Suite) TestNoGID() {
	msg := make([]byte, 1+1+8+8+8+4+1)
	msg[0] = uint8(MessageTypePrepare)
	msg[1] = 0
	prepareLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	xid := s.newXid()
	bigEndian.PutUint64(msg[1+1:], uint64(prepareLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], prepareTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8:], xid)
	msg[1+1+8+8+8+4] = 0

	expected := &PrepareMessageV3{
		Flags:             0,
		PrepareLSN:        prepareLSN,
		TransactionEndLSN: transactionEndLSN,
		PrepareTime:       prepareTime,
		Xid:               xid,
	}
	expected.msgType = MessageTypePrepare
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*PrepareMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func TestCommitPreparedV3Suite(t *testing.T) {
	suite.Run(t, new(commitPreparedMessageV3Suite))
}

type commitPreparedMessageV3Suite struct {
	messageSuite
}

func (s *commitPreparedMessageV3Suite) Test() {
	msg := make([]byte, 1+1+8+8+8+4+4+1)
	msg[0] = uint8(MessageTypeCommitPrepared)
	msg[1] = 0
	commitLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	commitTime, commitTimeU64 := s.newTime()
	xid := s.newXid()
	gid := "test"
	bigEndian.PutUint64(msg[1+1:], uint64(commitLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], commitTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8:], xid)
	s.putString(msg[1+1+8+8+8+4:], gid)

	expected := &CommitPreparedMessageV3{
		Flags:             0,
		CommitLSN:         commitLSN,
		TransactionEndLSN: transactionEndLSN,
		CommitTime:        commitTime,
		Xid:               xid,
		Gid:               gid,
	}
	expected.msgType = MessageTypeCommitPrepared
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*CommitPreparedMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func (s *commitPreparedMessageV3Suite) TestNoGID() {
	msg := make([]byte, 1+1+8+8+8+4+1)
	msg[0] = uint8(MessageTypeCommitPrepared)
	msg[1] = 0
	commitLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	commitTime, commitTimeU64 := s.newTime()
	xid := s.newXid()
	bigEndian.PutUint64(msg[1+1:], uint64(commitLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], commitTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8:], xid)
	msg[1+1+8+8+8+4] = 0

	expected := &CommitPreparedMessageV3{
		Flags:             0,
		CommitLSN:         commitLSN,
		TransactionEndLSN: transactionEndLSN,
		CommitTime:        commitTime,
		Xid:               xid,
	}
	expected.msgType = MessageTypeCommitPrepared
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*CommitPreparedMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func TestRollbackPreparedV3Suite(t *testing.T) {
	suite.Run(t, new(rollbackPreparedMessageV3Suite))
}

type rollbackPreparedMessageV3Suite struct {
	messageSuite
}

func (s *rollbackPreparedMessageV3Suite) Test() {
	msg := make([]byte, 1+1+8+8+8+8+4+4+1)
	msg[0] = uint8(MessageTypeRollbackPrepared)
	msg[1] = 0
	transactionEndLSN := s.newLSN()
	transactionRollbackLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	rollbackTime, rollbackTimeU64 := s.newTime()
	xid := s.newXid()
	gid := "test"
	bigEndian.PutUint64(msg[1+1:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionRollbackLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], prepareTimeU64)
	bigEndian.PutUint64(msg[1+1+8+8+8:], rollbackTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8+8:], xid)
	s.putString(msg[1+1+8+8+8+8+4:], gid)

	expected := &RollbackPreparedMessageV3{
		Flags:                  0,
		TransactionEndLSN:      transactionEndLSN,
		TransactionRollbackLSN: transactionRollbackLSN,
		PrepareTime:            prepareTime,
		RollbackTime:           rollbackTime,
		Xid:                    xid,
		Gid:                    gid,
	}
	expected.msgType = MessageTypeRollbackPrepared
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*RollbackPreparedMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func (s *rollbackPreparedMessageV3Suite) TestNoGID() {
	msg := make([]byte, 1+1+8+8+8+8+4+1)
	msg[0] = uint8(MessageTypeRollbackPrepared)
	msg[1] = 0
	transactionEndLSN := s.newLSN()
	transactionRollbackLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	rollbackTime, rollbackTimeU64 := s.newTime()
	xid := s.newXid()
	bigEndian.PutUint64(msg[1+1:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionRollbackLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], prepareTimeU64)
	bigEndian.PutUint64(msg[1+1+8+8+8:], rollbackTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8+8:], xid)
	msg[1+1+8+8+8+8+4] = 0

	expected := &RollbackPreparedMessageV3{
		Flags:                  0,
		TransactionEndLSN:      transactionEndLSN,
		TransactionRollbackLSN: transactionRollbackLSN,
		PrepareTime:            prepareTime,
		RollbackTime:           rollbackTime,
		Xid:                    xid,
	}
	expected.msgType = MessageTypeRollbackPrepared
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*RollbackPreparedMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func TestStreamPrepareMessageV3Suite(t *testing.T) {
	suite.Run(t, new(streamPrepareMessageV3Suite))
}

type streamPrepareMessageV3Suite struct {
	messageSuite
}

func (s *streamPrepareMessageV3Suite) Test() {
	msg := make([]byte, 1+1+8+8+8+4+4+1)
	msg[0] = uint8(MessageTypeStreamPrepare)
	msg[1] = 0
	prepareLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	xid := s.newXid()
	gid := "test"
	bigEndian.PutUint64(msg[1+1:], uint64(prepareLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], prepareTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8:], xid)
	s.putString(msg[1+1+8+8+8+4:], gid)

	expected := &StreamPrepareMessageV3{
		Flags:             0,
		PrepareLSN:        prepareLSN,
		TransactionEndLSN: transactionEndLSN,
		PrepareTime:       prepareTime,
		Xid:               xid,
		Gid:               gid,
	}
	expected.msgType = MessageTypeStreamPrepare
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*StreamPrepareMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}

func (s *streamPrepareMessageV3Suite) TestNoGID() {
	msg := make([]byte, 1+1+8+8+8+4+1)
	msg[0] = uint8(MessageTypeStreamPrepare)
	msg[1] = 0
	prepareLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	prepareTime, prepareTimeU64 := s.newTime()
	xid := s.newXid()
	bigEndian.PutUint64(msg[1+1:], uint64(prepareLSN))
	bigEndian.PutUint64(msg[1+1+8:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[1+1+8+8:], prepareTimeU64)
	bigEndian.PutUint32(msg[1+1+8+8+8:], xid)
	msg[1+1+8+8+8+4] = 0

	expected := &StreamPrepareMessageV3{
		Flags:             0,
		PrepareLSN:        prepareLSN,
		TransactionEndLSN: transactionEndLSN,
		PrepareTime:       prepareTime,
		Xid:               xid,
	}
	expected.msgType = MessageTypeStreamPrepare
	s.assertV1NotSupported(msg)
	s.assertV2NotSupported(msg)

	for _, inStream := range []bool{false, true} {
		m, err := ParseV3(msg, inStream)
		s.NoError(err)
		logicalDecodingMsg, ok := m.(*StreamPrepareMessageV3)
		s.True(ok)
		s.Equal(expected, logicalDecodingMsg)
	}
}
