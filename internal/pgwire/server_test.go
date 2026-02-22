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
	require.Equal(t, byte('K'), typeByte)
	require.Len(t, payload, 8)

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

func TestServer_ExtendedQueryProtocol_UnnamedStatementPortal(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, principal string, sqlQuery string) (*QueryResult, error) {
		require.Equal(t, "duck", principal)
		require.Equal(t, "SELECT 7", sqlQuery)
		return &QueryResult{
			Columns: []string{"value"},
			Rows:    [][]interface{}{{"7"}},
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

	for i := 0; i < 5; i++ {
		_, _ = readPGMessage(t, conn)
	}

	_, err = conn.Write(parsePacket(t, "SELECT 7"))
	require.NoError(t, err)
	typeByte, payload := readPGMessage(t, conn)
	require.Equal(t, byte('1'), typeByte)
	require.Len(t, payload, 0)

	_, err = conn.Write(bindPacketNoParams(t))
	require.NoError(t, err)
	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('2'), typeByte)
	require.Len(t, payload, 0)

	_, err = conn.Write(describePortalPacket(t))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('n'), typeByte)

	_, err = conn.Write(executePacket(t))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('T'), typeByte)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('D'), typeByte)
	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('C'), typeByte)
	require.Contains(t, string(payload), "SELECT 1")

	_, err = conn.Write(syncPacket(t))
	require.NoError(t, err)
	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('Z'), typeByte)
	require.Equal(t, []byte{'I'}, payload)
}

func TestServer_ExtendedQueryProtocol_WithTextParameter(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, principal string, sqlQuery string) (*QueryResult, error) {
		require.Equal(t, "duck", principal)
		require.Equal(t, "SELECT '7'::INT", sqlQuery)
		return &QueryResult{
			Columns: []string{"value"},
			Rows:    [][]interface{}{{"7"}},
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
	for i := 0; i < 5; i++ {
		_, _ = readPGMessage(t, conn)
	}

	_, err = conn.Write(parsePacket(t, "SELECT $1::INT"))
	require.NoError(t, err)
	typeByte, _ := readPGMessage(t, conn)
	require.Equal(t, byte('1'), typeByte)

	_, err = conn.Write(bindPacketWithTextParam(t, "7"))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('2'), typeByte)

	_, err = conn.Write(executePacket(t))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('T'), typeByte)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('D'), typeByte)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('C'), typeByte)

	_, err = conn.Write(syncPacket(t))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('Z'), typeByte)
}

func TestServer_ExtendedQueryProtocol_WithBinaryIntParameter(t *testing.T) {
	srv := NewServer("127.0.0.1:0", nil, func(_ context.Context, principal string, sqlQuery string) (*QueryResult, error) {
		require.Equal(t, "duck", principal)
		require.Equal(t, "SELECT 7::INT", sqlQuery)
		return &QueryResult{
			Columns: []string{"value"},
			Rows:    [][]interface{}{{"7"}},
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
	for i := 0; i < 5; i++ {
		_, _ = readPGMessage(t, conn)
	}

	_, err = conn.Write(parsePacketWithParamType(t, "SELECT $1::INT", 23))
	require.NoError(t, err)
	typeByte, _ := readPGMessage(t, conn)
	require.Equal(t, byte('1'), typeByte)

	_, err = conn.Write(bindPacketWithBinaryInt32Param(t, 7))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('2'), typeByte)

	_, err = conn.Write(executePacket(t))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('T'), typeByte)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('D'), typeByte)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('C'), typeByte)

	_, err = conn.Write(syncPacket(t))
	require.NoError(t, err)
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('Z'), typeByte)
}

func TestServer_CancelRequest_CancelsInFlightQuery(t *testing.T) {
	queryStarted := make(chan struct{}, 1)
	srv := NewServer("127.0.0.1:0", nil, func(ctx context.Context, principal string, sqlQuery string) (*QueryResult, error) {
		require.Equal(t, "duck", principal)
		require.Equal(t, "SELECT 1", sqlQuery)
		queryStarted <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
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

	for i := 0; i < 3; i++ {
		_, _ = readPGMessage(t, conn)
	}
	typeByte, payload := readPGMessage(t, conn)
	require.Equal(t, byte('K'), typeByte)
	require.Len(t, payload, 8)
	processID := binary.BigEndian.Uint32(payload[0:4])
	secretKey := binary.BigEndian.Uint32(payload[4:8])
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('Z'), typeByte)

	_, err = conn.Write(simpleQueryPacket(t, "SELECT 1"))
	require.NoError(t, err)

	select {
	case <-queryStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("query did not start")
	}

	cancelConn, err := net.DialTimeout("tcp", srv.Addr(), time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cancelConn.Close() })
	_, err = cancelConn.Write(cancelRequestPacket(t, processID, secretKey))
	require.NoError(t, err)

	typeByte, payload = readPGMessage(t, conn)
	require.Equal(t, byte('E'), typeByte)
	require.Contains(t, string(payload), "canceled")
	typeByte, _ = readPGMessage(t, conn)
	require.Equal(t, byte('Z'), typeByte)
}

func TestDecodeBinaryBindValue_CommonScalarTypes(t *testing.T) {
	t.Run("uuid", func(t *testing.T) {
		value, err := decodeBinaryBindValue(2950, []byte{0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff})
		require.NoError(t, err)
		require.Equal(t, "'00112233-4455-6677-8899-aabbccddeeff'::UUID", value)
	})

	t.Run("date", func(t *testing.T) {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, uint32(int32(1)))
		value, err := decodeBinaryBindValue(1082, buf)
		require.NoError(t, err)
		require.Equal(t, "'2000-01-02'::DATE", value)
	})

	t.Run("timestamp", func(t *testing.T) {
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(int64(1_000_000)))
		value, err := decodeBinaryBindValue(1114, buf)
		require.NoError(t, err)
		require.Equal(t, "'2000-01-01 00:00:01'::TIMESTAMP", value)
	})

	t.Run("numeric", func(t *testing.T) {
		value, err := decodeBinaryBindValue(1700, mustEncodeNumericBinary(t, []int16{12, 3456}, 0, 0x0000, 2))
		require.NoError(t, err)
		require.Equal(t, "12.34", value)
	})
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

func parsePacket(t *testing.T, q string) []byte {
	t.Helper()
	payload := bytes.NewBuffer(nil)
	_, err := payload.Write([]byte{0})
	require.NoError(t, err)
	_, err = payload.Write(append([]byte(q), 0))
	require.NoError(t, err)
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(0)))

	buf := bytes.NewBuffer(make([]byte, 0, 1+4+payload.Len()))
	require.NoError(t, buf.WriteByte('P'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+payload.Len())))
	_, err = buf.Write(payload.Bytes())
	require.NoError(t, err)
	return buf.Bytes()
}

func parsePacketWithParamType(t *testing.T, q string, oid uint32) []byte {
	t.Helper()
	payload := bytes.NewBuffer(nil)
	_, err := payload.Write([]byte{0})
	require.NoError(t, err)
	_, err = payload.Write(append([]byte(q), 0))
	require.NoError(t, err)
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(1)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, oid))

	buf := bytes.NewBuffer(make([]byte, 0, 1+4+payload.Len()))
	require.NoError(t, buf.WriteByte('P'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+payload.Len())))
	_, err = buf.Write(payload.Bytes())
	require.NoError(t, err)
	return buf.Bytes()
}

func bindPacketNoParams(t *testing.T) []byte {
	t.Helper()
	payload := bytes.NewBuffer(nil)
	_, err := payload.Write([]byte{0})
	require.NoError(t, err)
	_, err = payload.Write([]byte{0})
	require.NoError(t, err)
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(0)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(0)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(0)))

	buf := bytes.NewBuffer(make([]byte, 0, 1+4+payload.Len()))
	require.NoError(t, buf.WriteByte('B'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+payload.Len())))
	_, err = buf.Write(payload.Bytes())
	require.NoError(t, err)
	return buf.Bytes()
}

func bindPacketWithTextParam(t *testing.T, value string) []byte {
	t.Helper()
	payload := bytes.NewBuffer(nil)
	_, err := payload.Write([]byte{0})
	require.NoError(t, err)
	_, err = payload.Write([]byte{0})
	require.NoError(t, err)
	// parameter format codes: one text format code (0)
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(1)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(0)))
	// number of parameters
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(1)))
	// parameter value
	require.NoError(t, binary.Write(payload, binary.BigEndian, int32(len(value))))
	_, err = payload.Write([]byte(value))
	require.NoError(t, err)
	// result format codes: none
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(0)))

	buf := bytes.NewBuffer(make([]byte, 0, 1+4+payload.Len()))
	require.NoError(t, buf.WriteByte('B'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+payload.Len())))
	_, err = buf.Write(payload.Bytes())
	require.NoError(t, err)
	return buf.Bytes()
}

func bindPacketWithBinaryInt32Param(t *testing.T, value int32) []byte {
	t.Helper()
	payload := bytes.NewBuffer(nil)
	_, err := payload.Write([]byte{0})
	require.NoError(t, err)
	_, err = payload.Write([]byte{0})
	require.NoError(t, err)
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(1)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(1)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(1)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, int32(4)))
	require.NoError(t, binary.Write(payload, binary.BigEndian, value))
	require.NoError(t, binary.Write(payload, binary.BigEndian, uint16(0)))

	buf := bytes.NewBuffer(make([]byte, 0, 1+4+payload.Len()))
	require.NoError(t, buf.WriteByte('B'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+payload.Len())))
	_, err = buf.Write(payload.Bytes())
	require.NoError(t, err)
	return buf.Bytes()
}

func describePortalPacket(t *testing.T) []byte {
	t.Helper()
	payload := []byte{'P', 0}
	buf := bytes.NewBuffer(make([]byte, 0, 1+4+len(payload)))
	require.NoError(t, buf.WriteByte('D'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+len(payload))))
	_, err := buf.Write(payload)
	require.NoError(t, err)
	return buf.Bytes()
}

func executePacket(t *testing.T) []byte {
	t.Helper()
	payload := bytes.NewBuffer(nil)
	_, err := payload.Write([]byte{0})
	require.NoError(t, err)
	require.NoError(t, binary.Write(payload, binary.BigEndian, int32(0)))

	buf := bytes.NewBuffer(make([]byte, 0, 1+4+payload.Len()))
	require.NoError(t, buf.WriteByte('E'))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(4+payload.Len())))
	_, err = buf.Write(payload.Bytes())
	require.NoError(t, err)
	return buf.Bytes()
}

func syncPacket(t *testing.T) []byte {
	t.Helper()
	return []byte{'S', 0, 0, 0, 4}
}

func cancelRequestPacket(t *testing.T, processID, secretKey uint32) []byte {
	t.Helper()
	buf := bytes.NewBuffer(make([]byte, 0, 16))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int32(16)))
	require.NoError(t, binary.Write(buf, binary.BigEndian, pgCancelReqCode))
	require.NoError(t, binary.Write(buf, binary.BigEndian, processID))
	require.NoError(t, binary.Write(buf, binary.BigEndian, secretKey))
	return buf.Bytes()
}

func mustEncodeNumericBinary(t *testing.T, digits []int16, weight int16, sign uint16, dscale int16) []byte {
	t.Helper()
	buf := bytes.NewBuffer(make([]byte, 0, 8+(len(digits)*2)))
	require.NoError(t, binary.Write(buf, binary.BigEndian, int16(len(digits))))
	require.NoError(t, binary.Write(buf, binary.BigEndian, weight))
	require.NoError(t, binary.Write(buf, binary.BigEndian, sign))
	require.NoError(t, binary.Write(buf, binary.BigEndian, dscale))
	for _, d := range digits {
		require.NoError(t, binary.Write(buf, binary.BigEndian, d))
	}
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
