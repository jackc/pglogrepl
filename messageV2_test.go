package pglogrepl

import (
	"testing"

	"github.com/stretchr/testify/suite"
)

func TestLogicalDecodingMessageV2Suite(t *testing.T) {
	suite.Run(t, new(logicalDecodingMessageSuiteV2))
}

type logicalDecodingMessageSuiteV2 struct {
	messageSuite
}

func (s *logicalDecodingMessageSuiteV2) Test() {
	msg := make([]byte, 1+4+1+8+5+4+5)
	msg[0] = 'M'
	xid := s.newXid()
	bigEndian.PutUint32(msg[1:], xid)

	expected := s.putMessageTestData(msg[5:])

	expectedV2 := &LogicalDecodingMessageV2{
		LogicalDecodingMessage:   *expected,
		InStreamMessageV2WithXid: InStreamMessageV2WithXid{Xid: xid},
	}
	expectedV2.msgType = MessageTypeMessage

	m, err := ParseV2(msg, true)
	s.NoError(err)
	logicalDecodingMsg, ok := m.(*LogicalDecodingMessageV2)
	s.True(ok)

	s.Equal(expectedV2, logicalDecodingMsg)
}

func (s *logicalDecodingMessageSuiteV2) TestNoStream() {
	msg := make([]byte, 1+1+8+5+4+5)
	msg[0] = 'M'
	expected := s.putMessageTestData(msg[1:])
	expected.msgType = MessageTypeMessage
	m, err := ParseV2(msg, false)
	s.NoError(err)
	logicalDecodingMsg, ok := m.(*LogicalDecodingMessageV2)
	s.True(ok)

	s.Equal(uint32(0), logicalDecodingMsg.Xid)
	s.Equal(expected, &logicalDecodingMsg.LogicalDecodingMessage)
}

func TestStreamStartV2Suite(t *testing.T) {
	suite.Run(t, new(streamStartSuite))
}

type streamStartSuite struct {
	messageSuite
}

func (s *streamStartSuite) Test() {
	msg := make([]byte, 1+4+1)
	msg[0] = 'S'
	xid := s.newXid()
	firstSeg := byte(1)
	bigEndian.PutUint32(msg[1:], xid)
	msg[5] = firstSeg

	expected := &StreamStartMessageV2{
		Xid:          xid,
		FirstSegment: firstSeg,
	}
	expected.msgType = MessageTypeStreamStart
	s.assertV1NotSupported(msg)

	m, err := ParseV2(msg, false)
	s.NoError(err)
	startMsg, ok := m.(*StreamStartMessageV2)
	s.True(ok)

	s.Equal(expected, startMsg)
}

func TestStreamStopV2Suite(t *testing.T) {
	suite.Run(t, new(streamStopSuite))
}

type streamStopSuite struct {
	messageSuite
}

func (s *streamStopSuite) Test() {
	msg := make([]byte, 1)
	msg[0] = 'E'

	expected := &StreamStopMessageV2{}
	expected.msgType = MessageTypeStreamStop

	s.assertV1NotSupported(msg)
	m, err := ParseV2(msg, false)
	s.NoError(err)
	stopMsg, ok := m.(*StreamStopMessageV2)
	s.True(ok)

	s.Equal(expected, stopMsg)
}

func TestStreamCommitV2Suite(t *testing.T) {
	suite.Run(t, new(streamCommitSuite))
}

type streamCommitSuite struct {
	messageSuite
}

func (s *streamCommitSuite) Test() {
	msg := make([]byte, 1+4+1+8+8+8)
	xid := s.newXid()
	flags := uint8(0)
	commitLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	commitTime, pgCommitTime := s.newTime()

	msg[0] = 'c'
	bigEndian.PutUint32(msg[1:], xid)
	msg[5] = flags
	bigEndian.PutUint64(msg[6:], uint64(commitLSN))
	bigEndian.PutUint64(msg[14:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[22:], pgCommitTime)

	expected := &StreamCommitMessageV2{
		Xid:               xid,
		Flags:             flags,
		CommitLSN:         commitLSN,
		TransactionEndLSN: transactionEndLSN,
		CommitTime:        commitTime,
	}
	expected.msgType = MessageTypeStreamCommit

	s.assertV1NotSupported(msg)

	m, err := ParseV2(msg, false)
	s.NoError(err)
	streamCommitMsg, ok := m.(*StreamCommitMessageV2)
	s.True(ok)
	s.Equal(expected, streamCommitMsg)
}

func TestStreamAbortV2Suite(t *testing.T) {
	suite.Run(t, new(streamAbortSuite))
}

type streamAbortSuite struct {
	messageSuite
}

func (s *streamAbortSuite) Test() {
	msg := make([]byte, 1+4+4)

	xid := s.newXid()
	subXid := s.newXid()

	msg[0] = 'A'
	bigEndian.PutUint32(msg[1:], xid)
	bigEndian.PutUint32(msg[5:], subXid)

	expected := &StreamAbortMessageV2{
		Xid:    xid,
		SubXid: subXid,
	}
	expected.msgType = MessageTypeStreamAbort

	s.assertV1NotSupported(msg)

	m, err := ParseV2(msg, false)
	s.NoError(err)
	streamAbortMsg, ok := m.(*StreamAbortMessageV2)
	s.True(ok)
	s.Equal(expected, streamAbortMsg)
}

func TestRelationMessageV2Suite(t *testing.T) {
	suite.Run(t, new(relationMessageV2Suite))
}

type relationMessageV2Suite struct {
	messageSuite
}

func (s *relationMessageV2Suite) Test() {
	msg, expected := s.createRelationTestData()

	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	relMsg, ok := m.(*RelationMessageV2)
	s.True(ok)
	s.Equal(xid, relMsg.Xid)
	s.Equal(expected, &relMsg.RelationMessage)
}

func (s *relationMessageV2Suite) TestNoStream() {
	msg, expected := s.createRelationTestData()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	relMsg, ok := m.(*RelationMessageV2)
	s.True(ok)
	s.Equal(uint32(0), relMsg.Xid)
	s.Equal(expected, &relMsg.RelationMessage)
}

func TestTypeMessageV2Suite(t *testing.T) {
	suite.Run(t, new(typeMessageV2Suite))
}

type typeMessageV2Suite struct {
	messageSuite
}

func (s *typeMessageV2Suite) Test() {
	msg, expected := s.createTypeTestData()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	typeMsg, ok := m.(*TypeMessageV2)
	s.True(ok)
	s.Equal(xid, typeMsg.Xid)
	s.Equal(expected, &typeMsg.TypeMessage)
}

func (s *typeMessageV2Suite) TestNoStream() {
	msg, expected := s.createTypeTestData()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	typeMsg, ok := m.(*TypeMessageV2)
	s.True(ok)
	s.Equal(uint32(0), typeMsg.Xid)
	s.Equal(expected, &typeMsg.TypeMessage)
}

func TestInsertMessageV2Suite(t *testing.T) {
	suite.Run(t, new(insertMessageV2Suite))
}

type insertMessageV2Suite struct {
	messageSuite
}

func (s *insertMessageV2Suite) Test() {
	msg, expected := s.createInsertTestData()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	insertMsg, ok := m.(*InsertMessageV2)
	s.True(ok)
	s.Equal(xid, insertMsg.Xid)
	s.Equal(expected, &insertMsg.InsertMessage)
}

func (s *insertMessageV2Suite) TestNoStream() {
	msg, expected := s.createInsertTestData()

	m, err := ParseV2(msg, false)
	s.NoError(err)
	insertMsg, ok := m.(*InsertMessageV2)
	s.True(ok)
	s.Equal(uint32(0), insertMsg.Xid)
	s.Equal(expected, &insertMsg.InsertMessage)
}

func TestUpdateMessageV2Suite(t *testing.T) {
	suite.Run(t, new(updateMessageV2Suite))
}

type updateMessageV2Suite struct {
	messageSuite
}

func (s *updateMessageV2Suite) TestUpdateV2WithOldTupleTypeK() {
	msg, expected := s.createUpdateTestDataTypeK()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessageV2)
	s.True(ok)
	s.Equal(xid, updateMsg.Xid)
	s.Equal(expected, &updateMsg.UpdateMessage)
}

func (s *updateMessageV2Suite) TestUpdateV2WithOldTupleTypeKNoStream() {
	msg, expected := s.createUpdateTestDataTypeK()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessageV2)
	s.True(ok)
	s.Equal(uint32(0), updateMsg.Xid)
	s.Equal(expected, &updateMsg.UpdateMessage)
}

func (s *updateMessageV2Suite) TestUpdateV2WithOldTupleTypeO() {
	msg, expected := s.createUpdateTestDataTypeO()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessageV2)
	s.True(ok)
	s.Equal(xid, updateMsg.Xid)
	s.Equal(expected, &updateMsg.UpdateMessage)
}

func (s *updateMessageV2Suite) TestUpdateV2WithOldTupleTypeONoStream() {
	msg, expected := s.createUpdateTestDataTypeO()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessageV2)
	s.True(ok)
	s.Equal(uint32(0), updateMsg.Xid)
	s.Equal(expected, &updateMsg.UpdateMessage)
}

func (s *updateMessageV2Suite) TestUpdateV2WithoutOldTuple() {
	msg, expected := s.createUpdateTestDataWithoutOldTuple()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessageV2)
	s.True(ok)
	s.Equal(xid, updateMsg.Xid)
	s.Equal(expected, &updateMsg.UpdateMessage)
}

func (s *updateMessageV2Suite) TestUpdateV2WithoutOldTupleNoStream() {
	msg, expected := s.createUpdateTestDataWithoutOldTuple()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	updateMsg, ok := m.(*UpdateMessageV2)
	s.True(ok)
	s.Equal(uint32(0), updateMsg.Xid)
	s.Equal(expected, &updateMsg.UpdateMessage)
}

func TestDeleteMessageV2Suite(t *testing.T) {
	suite.Run(t, new(deleteMessageV2Suite))
}

type deleteMessageV2Suite struct {
	messageSuite
}

func (s *deleteMessageV2Suite) TestV2WithOldTupleTypeK() {
	msg, expected := s.createDeleteTestDataTypeK()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	deleteMsg, ok := m.(*DeleteMessageV2)
	s.True(ok)
	s.Equal(xid, deleteMsg.Xid)
	s.Equal(expected, &deleteMsg.DeleteMessage)
}

func (s *deleteMessageV2Suite) TestV2WithOldTupleTypeKNoStream() {
	msg, expected := s.createDeleteTestDataTypeK()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	deleteMsg, ok := m.(*DeleteMessageV2)
	s.True(ok)
	s.Equal(uint32(0), deleteMsg.Xid)
	s.Equal(expected, &deleteMsg.DeleteMessage)
}

func (s *deleteMessageV2Suite) TestV2WithOldTupleTypeO() {
	msg, expected := s.createDeleteTestDataTypeO()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	deleteMsg, ok := m.(*DeleteMessageV2)
	s.True(ok)
	s.Equal(xid, deleteMsg.Xid)
	s.Equal(expected, &deleteMsg.DeleteMessage)
}

func (s *deleteMessageV2Suite) TestV2WithOldTupleTypeONoStream() {
	msg, expected := s.createDeleteTestDataTypeO()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	deleteMsg, ok := m.(*DeleteMessageV2)
	s.True(ok)
	s.Equal(uint32(0), deleteMsg.Xid)
	s.Equal(expected, &deleteMsg.DeleteMessage)
}

func TestTruncateMessageV2Suite(t *testing.T) {
	suite.Run(t, new(truncateMessageSuiteV2))
}

type truncateMessageSuiteV2 struct {
	messageSuite
}

func (s *truncateMessageSuiteV2) Test() {
	msg, expected := s.createTruncateTestData()
	msgV2, xid := s.insertXid(msg)

	m, err := ParseV2(msgV2, true)
	s.NoError(err)
	truncateMsg, ok := m.(*TruncateMessageV2)
	s.True(ok)
	s.Equal(xid, truncateMsg.Xid)
	s.Equal(expected, &truncateMsg.TruncateMessage)
}

func (s *truncateMessageSuiteV2) TestNoStream() {
	msg, expected := s.createTruncateTestData()
	m, err := ParseV2(msg, false)
	s.NoError(err)
	truncateMsg, ok := m.(*TruncateMessageV2)
	s.True(ok)
	s.Equal(uint32(0), truncateMsg.Xid)
	s.Equal(expected, &truncateMsg.TruncateMessage)
}
