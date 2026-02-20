package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestServer_StartAndShutdown(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, principal string, sqlQuery string) (*QueryResult, error) {
		require.Equal(t, "duck", principal)
		require.Equal(t, "SELECT 1", sqlQuery)
		return &QueryResult{
			Columns: []string{"value"},
			Rows:    [][]interface{}{{"1"}},
		}, nil
	})
	require.NoError(t, srv.Start())
	t.Cleanup(func() {
		_ = srv.Shutdown(context.Background())
	})

	conn, err := net.DialTimeout("tcp", srv.Addr(), time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = conn.Write(startupPacket(t))
	require.NoError(t, err)

	typeByte, payload := readPGMessage(t, conn)
	require.Equal(t, byte('R'), typeByte)
	require.Len(t, payload, 4)
	require.Equal(t, uint32(0), binary.BigEndian.Uint32(payload))

	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('S'), typeByte)
	require.Contains(t, string(payload), "server_version")

	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('S'), typeByte)
	require.Contains(t, string(payload), "client_encoding")

	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('Z'), typeByte)
	require.Equal(t, []byte{'I'}, payload)

	_, err = conn.Write(simpleQueryPacket(t, "SELECT 1"))
	require.NoError(t, err)

	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('T'), typeByte)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('D'), typeByte)
	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('C'), typeByte)
	require.Contains(t, string(payload), "SELECT 1")
	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('Z'), typeByte)
	require.Equal(t, []byte{'I'}, payload)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, srv.Shutdown(ctx))
}

func startupPacket(t *testing.T) []byte {
	t.Helper()

	params := []byte("user\x00duck\x00database\x00duck\x00\x00")
	buf := bytes.NewBuffer(make([]byte, 0, 8+len(params)))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(8+len(params))))
	require.NoError(t, binary.Write(buf, binary.BigEndian, pgProtocolVersion3))
	_, err := buf.Write(params)
	require.NoError(t, err)
	return buf.Bytes()
}

func simpleQueryPacket(t *testing.T, q string) []byte {
	t.Helper()
	payload := append([]byte(q), 0)
	buf := bytes.NewBuffer(make([]byte, 0, 1+4+len(payload)))
	require.NoError(t, buf.WriteByte('Q'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+len(payload))))
	_, err := buf.Write(payload)
	require.NoError(t, err)
	return buf.Bytes()
}

func readPGMessage(t *testing.T, conn net.Conn) (byte, []byte) {
	t.Helper()
	typeByte := make([]byte, 1)
	_, err := io.ReadFull(conn, typeByte)
	require.NoError(t, err)

	lenBuf := make([]byte, 4)
	_, err = io.ReadFull(conn, lenBuf)
	require.NoError(t, err)
	length := int(binary.BigEndian.Uint32(lenBuf))
	require.GreaterOrEqual(t, length, 4)

	payload := make([]byte, length-4)
	_, err = io.ReadFull(conn, payload)
	require.NoError(t, err)
	return typeByte[0], payload
}
