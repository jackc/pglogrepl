package pglogrepl

import (
	"encoding/binary"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

var bigEndian = binary.BigEndian

type messageSuite struct {
	suite.Suite
}

func (s *messageSuite) R() *require.Assertions {
	return s.Require()
}

func (s *messageSuite) Equal(e, a interface{}, args ...interface{}) {
	s.R().Equal(e, a, args...)
}

func (s *messageSuite) NoError(err error) {
	s.R().NoError(err)
}

func (s *messageSuite) True(value bool) {
	s.R().True(value)
}

func (s *messageSuite) newLSN() LSN {
	return LSN(rand.Int63())
}

func (s *messageSuite) newXid() uint32 {
	return uint32(rand.Int31())
}

func (s *messageSuite) newTime() (time.Time, uint64) {
	// Postgres time format only support millisecond accuracy.
	now := time.Now().Truncate(time.Millisecond)
	return now, uint64(timeToPgTime(now))
}

func (s *messageSuite) putString(dst []byte, value string) int {
	copy(dst, []byte(value))
	dst[len(value)] = byte(0)
	return len(value) + 1
}

func TestBeginMessageSuite(t *testing.T) {
	suite.Run(t, new(beginMessageSuite))
}

type beginMessageSuite struct {
	messageSuite
}

func (s *beginMessageSuite) Test() {
	finalLSN := s.newLSN()
	commitTime, pgCommitTime := s.newTime()
	xid := s.newXid()

	msg := make([]byte, 1+8+8+4)
	msg[0] = 'B'
	bigEndian.PutUint64(msg[1:], uint64(finalLSN))
	bigEndian.PutUint64(msg[9:], pgCommitTime)
	bigEndian.PutUint32(msg[17:], xid)

	m, err := Parse(msg)
	s.NoError(err)
	beginMsg, ok := m.(*BeginMessage)
	s.True(ok)

	expected := &BeginMessage{
		FinalLSN:   finalLSN,
		CommitTime: commitTime,
		Xid:        xid,
	}
	expected.msgType = 'B'
	s.Equal(expected, beginMsg)
}

func TestCommitMessage(t *testing.T) {
	suite.Run(t, new(commitMessageSuite))
}

type commitMessageSuite struct {
	messageSuite
}

func (s *commitMessageSuite) Test() {
	flags := uint8(0)
	commitLSN := s.newLSN()
	transactionEndLSN := s.newLSN()
	commitTime, pgCommitTime := s.newTime()

	msg := make([]byte, 1+1+8+8+8)
	msg[0] = 'C'
	msg[1] = flags
	bigEndian.PutUint64(msg[2:], uint64(commitLSN))
	bigEndian.PutUint64(msg[10:], uint64(transactionEndLSN))
	bigEndian.PutUint64(msg[18:], pgCommitTime)

	m, err := Parse(msg)
	s.NoError(err)
	commitMsg, ok := m.(*CommitMessage)
	s.True(ok)

	expected := &CommitMessage{
		Flags:             0,
		CommitLSN:         commitLSN,
		TransactionEndLSN: transactionEndLSN,
		CommitTime:        commitTime,
	}
	expected.msgType = 'C'
	s.Equal(expected, commitMsg)
}

func TestOriginMessage(t *testing.T) {
	suite.Run(t, new(originMessageSuite))
}

type originMessageSuite struct {
	messageSuite
}

func (s *originMessageSuite) Test() {
	commitLSN := s.newLSN()
	name := "someorigin"

	msg := make([]byte, 1+8+len(name)+1) // 1 byte for \0
	msg[0] = 'O'
	bigEndian.PutUint64(msg[1:], uint64(commitLSN))
	s.putString(msg[9:], name)

	m, err := Parse(msg)
	s.NoError(err)
	originMsg, ok := m.(*OriginMessage)
	s.True(ok)

	expected := &OriginMessage{
		CommitLSN: commitLSN,
		Name:      name,
	}
	expected.msgType = 'O'
	s.Equal(expected, originMsg)
}

func TestRelationMessageSuite(t *testing.T) {
	suite.Run(t, new(relationMessageSuite))
}

type relationMessageSuite struct {
	messageSuite
}

func (s *relationMessageSuite) Test() {
	relationID := uint32(rand.Int31())
	namespace := "public"
	relationName := "table1"
	col1 := "id"         // int8
	col2 := "name"       // text
	col3 := "created_at" // timestamptz

	col1Length := 1 + len(col1) + 1 + 4 + 4
	col2Length := 1 + len(col2) + 1 + 4 + 4
	col3Length := 1 + len(col3) + 1 + 4 + 4

	msg := make([]byte, 1+4+len(namespace)+1+len(relationName)+1+1+
		2+col1Length+col2Length+col3Length)
	msg[0] = 'R'
	off := 1
	bigEndian.PutUint32(msg[off:], relationID)
	off += 4
	off += s.putString(msg[off:], namespace)
	off += s.putString(msg[off:], relationName)
	msg[off] = 1
	off++
	bigEndian.PutUint16(msg[off:], 3)
	off += 2

	msg[off] = 1 // column id is key
	off++
	off += s.putString(msg[off:], col1)
	bigEndian.PutUint32(msg[off:], 20) // int8
	off += 4
	bigEndian.PutUint32(msg[off:], 0)
	off += 4

	msg[off] = 0
	off++
	off += s.putString(msg[off:], col2)
	bigEndian.PutUint32(msg[off:], 25) // text
	off += 4
	bigEndian.PutUint32(msg[off:], 0)
	off += 4

	msg[off] = 0
	off++
	off += s.putString(msg[off:], col3)
	bigEndian.PutUint32(msg[off:], 1184) // timestamptz
	off += 4
	bigEndian.PutUint32(msg[off:], 0)
	off += 4

	m, err := Parse(msg)
	s.NoError(err)
	relationMsg, ok := m.(*RelationMessage)
	s.True(ok)

	expected := &RelationMessage{
		RelationID:      relationID,
		Namespace:       namespace,
		RelationName:    relationName,
		ReplicaIdentity: 1,
		ColumnNum:       3,
		Columns: []*RelationMessageColumn{
			{
				Flags:        1,
				Name:         col1,
				DataType:     20,
				TypeModifier: 0,
			},
			{
				Flags:        0,
				Name:         col2,
				DataType:     25,
				TypeModifier: 0,
			},
			{
				Flags:        0,
				Name:         col3,
				DataType:     1184,
				TypeModifier: 0,
			},
		},
	}
	expected.msgType = 'R'
	s.Equal(expected, relationMsg)
}

func TestTypeMessageSuite(t *testing.T) {
	suite.Run(t, new(typeMessageSuite))
}

type typeMessageSuite struct {
	messageSuite
}

func (s *typeMessageSuite) Test() {
	dataType := uint32(1184) // timestamptz
	namespace := "public"
	name := "created_at"

	msg := make([]byte, 1+4+len(namespace)+1+len(name)+1)
	msg[0] = 'Y'
	off := 1
	bigEndian.PutUint32(msg[off:], dataType)
	off += 4
	off += s.putString(msg[off:], namespace)
	s.putString(msg[off:], name)

	m, err := Parse(msg)
	s.NoError(err)
	typeMsg, ok := m.(*TypeMessage)
	s.True(ok)

	expected := &TypeMessage{
		DataType:  dataType,
		Namespace: namespace,
		Name:      name,
	}
	expected.msgType = 'Y'
	s.Equal(expected, typeMsg)
}
