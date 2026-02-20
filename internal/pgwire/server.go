package pgwire

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
)

const (
	pgProtocolVersion3 int32 = 196608
	pgSSLRequestCode   int32 = 80877103
	pgCancelReqCode    int32 = 80877102
)

type QueryResult struct {
	Columns []string
	Rows    [][]interface{}
}

type QueryExecutor func(ctx context.Context, principal string, sqlQuery string) (*QueryResult, error)

// Server is a guarded PostgreSQL wire listener with preview-level simple-query support.
type Server struct {
	addr   string
	logger *slog.Logger
	query  QueryExecutor

	mu sync.Mutex
	ln net.Listener
	wg sync.WaitGroup
}

func NewServer(addr string, logger *slog.Logger, query QueryExecutor) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if query == nil {
		query = func(_ context.Context, _ string, _ string) (*QueryResult, error) {
			return nil, fmt.Errorf("pgwire query executor is not configured")
		}
	}
	return &Server{addr: addr, logger: logger, query: query}
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return fmt.Errorf("pgwire listener already started")
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen pgwire: %w", err)
	}
	s.ln = ln
	s.wg.Add(1)
	go s.acceptLoop()
	s.logger.Info("PG-wire listener enabled", "addr", ln.Addr().String())
	return nil
}

func (s *Server) Addr() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return ""
	}
	return s.ln.Addr().String()
}

func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	ln := s.ln
	s.ln = nil
	s.mu.Unlock()
	if ln == nil {
		return nil
	}
	if err := ln.Close(); err != nil {
		return fmt.Errorf("close pgwire listener: %w", err)
	}

	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("pgwire shutdown: %w", ctx.Err())
	}
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		ln := s.ln
		s.mu.Unlock()
		if ln == nil {
			return
		}

		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close() //nolint:errcheck
			s.handleConn(conn)
		}()
	}
}

func (s *Server) handleConn(conn net.Conn) {
	principal := ""

	for {
		length, code, err := readStartupHeader(conn)
		if err != nil {
			return
		}

		if length < 8 {
			_ = writePGError(conn, "invalid startup packet")
			return
		}

		switch code {
		case pgSSLRequestCode:
			if _, err := conn.Write([]byte{'N'}); err != nil {
				return
			}
			continue
		case pgCancelReqCode:
			return
		case pgProtocolVersion3:
			payloadSize := int(length) - 8
			payload := make([]byte, 0)
			if payloadSize > 0 {
				payload = make([]byte, payloadSize)
				if _, err := io.ReadFull(conn, payload); err != nil {
					return
				}
			}
			startupParams := parseStartupParams(payload)
			if u, ok := startupParams["user"]; ok {
				principal = u
			}
			if principal == "" {
				principal = "anonymous"
			}
			if err := writeAuthenticationOK(conn); err != nil {
				return
			}
			if err := writeParameterStatus(conn, "server_version", "16.0"); err != nil {
				return
			}
			if err := writeParameterStatus(conn, "client_encoding", "UTF8"); err != nil {
				return
			}
			if err := writeReadyForQuery(conn); err != nil {
				return
			}
			s.serveSimpleQueryLoop(conn, principal)
			return
		default:
			_ = writePGError(conn, "unsupported startup protocol")
			return
		}
	}
}

func (s *Server) serveSimpleQueryLoop(conn net.Conn, principal string) {
	for {
		msgType := make([]byte, 1)
		if _, err := io.ReadFull(conn, msgType); err != nil {
			return
		}

		var lenBuf [4]byte
		if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
			return
		}
		length := int(binary.BigEndian.Uint32(lenBuf[:]))
		if length < 4 {
			_ = writePGError(conn, "invalid frontend message")
			_ = writeReadyForQuery(conn)
			continue
		}

		payload := make([]byte, length-4)
		if _, err := io.ReadFull(conn, payload); err != nil {
			return
		}

		switch msgType[0] {
		case 'Q':
			s.handleSimpleQuery(conn, principal, payload)
		case 'X':
			return
		case 'S':
			_ = writeReadyForQuery(conn)
		default:
			_ = writePGError(conn, fmt.Sprintf("unsupported frontend message type %q", msgType[0]))
			_ = writeReadyForQuery(conn)
		}
	}
}

func (s *Server) handleSimpleQuery(conn net.Conn, principal string, payload []byte) {
	query := string(bytes.TrimSuffix(payload, []byte{0}))
	result, err := s.query(context.Background(), principal, query)
	if err != nil {
		_ = writePGError(conn, err.Error())
		_ = writeReadyForQuery(conn)
		return
	}

	if err := writeRowDescription(conn, result.Columns); err != nil {
		return
	}
	for _, row := range result.Rows {
		if err := writeDataRow(conn, row); err != nil {
			return
		}
	}
	if err := writeCommandComplete(conn, fmt.Sprintf("SELECT %d", len(result.Rows))); err != nil {
		return
	}
	_ = writeReadyForQuery(conn)
}

func readStartupHeader(r io.Reader) (int32, int32, error) {
	var header [8]byte
	if _, err := io.ReadFull(r, header[:]); err != nil {
		return 0, 0, err
	}
	length := int32(binary.BigEndian.Uint32(header[0:4]))
	code := int32(binary.BigEndian.Uint32(header[4:8]))
	return length, code, nil
}

func writePGError(conn net.Conn, message string) error {
	body := make([]byte, 0, 128)
	body = append(body, 'S')
	body = append(body, []byte("ERROR")...)
	body = append(body, 0)
	body = append(body, 'C')
	body = append(body, []byte("0A000")...)
	body = append(body, 0)
	body = append(body, 'M')
	body = append(body, []byte(message)...)
	body = append(body, 0)
	body = append(body, 0)

	packet := make([]byte, 1+4+len(body))
	packet[0] = 'E'
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)

	_, err := conn.Write(packet)
	return err
}

func writeAuthenticationOK(conn net.Conn) error {
	packet := make([]byte, 1+4+4)
	packet[0] = 'R'
	binary.BigEndian.PutUint32(packet[1:5], 8)
	binary.BigEndian.PutUint32(packet[5:9], 0)
	_, err := conn.Write(packet)
	return err
}

func writeParameterStatus(conn net.Conn, key, value string) error {
	body := make([]byte, 0, len(key)+len(value)+2)
	body = append(body, []byte(key)...)
	body = append(body, 0)
	body = append(body, []byte(value)...)
	body = append(body, 0)

	packet := make([]byte, 1+4+len(body))
	packet[0] = 'S'
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	_, err := conn.Write(packet)
	return err
}

func writeReadyForQuery(conn net.Conn) error {
	packet := []byte{'Z', 0, 0, 0, 5, 'I'}
	_, err := conn.Write(packet)
	return err
}

func writeRowDescription(conn net.Conn, columns []string) error {
	body := make([]byte, 0, 64)
	countBuf := make([]byte, 2)
	binary.BigEndian.PutUint16(countBuf, uint16(len(columns)))
	body = append(body, countBuf...)

	for _, col := range columns {
		body = append(body, []byte(col)...)
		body = append(body, 0)

		buf4 := make([]byte, 4)
		binary.BigEndian.PutUint32(buf4, 0)
		body = append(body, buf4...)

		buf2 := make([]byte, 2)
		binary.BigEndian.PutUint16(buf2, 0)
		body = append(body, buf2...)

		binary.BigEndian.PutUint32(buf4, 25)
		body = append(body, buf4...)

		binary.BigEndian.PutUint16(buf2, 0xFFFF)
		body = append(body, buf2...)

		binary.BigEndian.PutUint32(buf4, 0xFFFFFFFF)
		body = append(body, buf4...)

		binary.BigEndian.PutUint16(buf2, 0)
		body = append(body, buf2...)
	}

	packet := make([]byte, 1+4+len(body))
	packet[0] = 'T'
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	_, err := conn.Write(packet)
	return err
}

func writeDataRow(conn net.Conn, row []interface{}) error {
	body := make([]byte, 0, 64)
	buf2 := make([]byte, 2)
	binary.BigEndian.PutUint16(buf2, uint16(len(row)))
	body = append(body, buf2...)

	for _, value := range row {
		if value == nil {
			buf4 := make([]byte, 4)
			binary.BigEndian.PutUint32(buf4, 0xFFFFFFFF)
			body = append(body, buf4...)
			continue
		}

		text := fmt.Sprintf("%v", value)
		bytesValue := []byte(text)
		buf4 := make([]byte, 4)
		binary.BigEndian.PutUint32(buf4, uint32(len(bytesValue)))
		body = append(body, buf4...)
		body = append(body, bytesValue...)
	}

	packet := make([]byte, 1+4+len(body))
	packet[0] = 'D'
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	_, err := conn.Write(packet)
	return err
}

func writeCommandComplete(conn net.Conn, tag string) error {
	body := append([]byte(tag), 0)
	packet := make([]byte, 1+4+len(body))
	packet[0] = 'C'
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
	_, err := conn.Write(packet)
	return err
}

func parseStartupParams(payload []byte) map[string]string {
	params := map[string]string{}
	parts := bytes.Split(payload, []byte{0})
	for i := 0; i+1 < len(parts); i += 2 {
		k := string(parts[i])
		v := string(parts[i+1])
		if k == "" {
			break
		}
		params[k] = v
	}
	return params
}
