package log

import (
	api "github.com/DeshErBojhaa/powerlog/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for msg, fn := range map[string]func(t *testing.T, log *Log){
		"append and read success":    testAppendRead,
		"offset out of range error":  testOutOfRangeErr,
		"truncate":                   testTruncate,
		"full reader":                testReader,
		"init with existing segment": testInitExistng,
	} {
		t.Run(msg, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "log-test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)
			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	read, err := log.Read(off)
	require.NoError(t, err)
	require.Equal(t, rec.Value, read.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	read, err := log.Read(1)
	require.Nil(t, read)
	require.Error(t, err)
}

func testTruncate(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(rec)
		require.NoError(t, err)
	}
	err := log.Truncate(1)
	require.NoError(t, err)
	_, err = log.Read(0)
	require.Error(t, err)
}

func testReader(t *testing.T, log *Log) {
	rec := &api.Record{
		Value: []byte("hello world"),
	}
	off, err := log.Append(rec)
	require.NoError(t, err)
	require.Equal(t, uint64(0), off)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	require.NoError(t, err)

	read := &api.Record{}
	err = proto.Unmarshal(b[lenWidth:], read)
	require.NoError(t, err)
	require.Equal(t, rec.Value, read.Value)
}

func testInitExistng(t *testing.T, log *Log) {
	rec := &api.Record{Value: []byte("hello world")}
	for i := 0; i < 3; i++ {
		_, err := log.Append(rec)
		require.NoError(t, err)
	}
	require.NoError(t, log.Close())

	lowOff1, err := log.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowOff1)

	highOff1, err := log.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highOff1)

	log2, err := NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	lowOff2, err := log2.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, lowOff1, lowOff2)

	highOff2, err := log2.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, highOff1, highOff2)
}
