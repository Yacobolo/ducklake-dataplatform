package pgwire

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"duck-demo/internal/domain"
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

	mu            sync.Mutex
	ln            net.Listener
	wg            sync.WaitGroup
	queryMu       sync.Mutex
	activeQueries map[cancelKey]context.CancelFunc
}

type extendedState struct {
	unnamedStatement string
	unnamedPortal    string
	unnamedParams    []string
	unnamedParamOIDs []uint32
}

type backendKey struct {
	processID int32
	secretKey int32
}

type cancelKey struct {
	processID int32
	secretKey int32
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
	return &Server{addr: addr, logger: logger, query: query, activeQueries: make(map[cancelKey]context.CancelFunc)}
}

func (s *Server) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln != nil {
		return fmt.Errorf("pgwire listener already started")
	}

	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.addr)
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
	key := newBackendKey()

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
			payloadSize := int(length) - 8
			if payloadSize != 8 {
				return
			}
			payload := make([]byte, payloadSize)
			if _, err := io.ReadFull(conn, payload); err != nil {
				return
			}
			s.cancelQuery(binary.BigEndian.Uint32(payload[0:4]), binary.BigEndian.Uint32(payload[4:8]))
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
			u, ok := startupParams["user"]
			if !ok || strings.TrimSpace(u) == "" {
				_ = writePGErrorCode(conn, "startup user is required", "28000")
				return
			}
			principal = strings.TrimSpace(u)
			if err := writeAuthenticationOK(conn); err != nil {
				return
			}
			if err := writeParameterStatus(conn, "server_version", "16.0"); err != nil {
				return
			}
			if err := writeParameterStatus(conn, "client_encoding", "UTF8"); err != nil {
				return
			}
			if err := writeBackendKeyData(conn, key.processID, key.secretKey); err != nil {
				return
			}
			if err := writeReadyForQuery(conn); err != nil {
				return
			}
			s.serveSimpleQueryLoop(conn, principal, key)
			return
		default:
			_ = writePGError(conn, "unsupported startup protocol")
			return
		}
	}
}

func (s *Server) serveSimpleQueryLoop(conn net.Conn, principal string, key backendKey) {
	state := &extendedState{}

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
			s.handleSimpleQuery(conn, principal, payload, key)
		case 'P':
			s.handleParse(conn, state, payload)
		case 'B':
			s.handleBind(conn, state, payload)
		case 'D':
			s.handleDescribe(conn, state, payload)
		case 'E':
			s.handleExecute(conn, state, principal, payload, key)
		case 'C':
			s.handleClose(conn, state, payload)
		case 'H':
			// Flush: backend has no buffered output to force.
		case 'S':
			_ = writeReadyForQuery(conn)
		case 'X':
			return
		default:
			_ = writePGError(conn, fmt.Sprintf("unsupported frontend message type %q", msgType[0]))
			_ = writeReadyForQuery(conn)
		}
	}
}

func (s *Server) handleParse(conn net.Conn, state *extendedState, payload []byte) {
	offset := 0
	statementName, ok := readCString(payload, &offset)
	if !ok {
		_ = writePGError(conn, "invalid Parse message")
		return
	}
	query, ok := readCString(payload, &offset)
	if !ok {
		_ = writePGError(conn, "invalid Parse message")
		return
	}
	if statementName != "" {
		_ = writePGError(conn, "only unnamed prepared statement is supported")
		return
	}
	if len(payload[offset:]) < 2 {
		_ = writePGError(conn, "invalid Parse parameter metadata")
		return
	}
	numParamTypes := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if len(payload[offset:]) < numParamTypes*4 {
		_ = writePGError(conn, "invalid Parse parameter type list")
		return
	}
	state.unnamedStatement = query
	state.unnamedParamOIDs = nil
	if numParamTypes > 0 {
		state.unnamedParamOIDs = make([]uint32, numParamTypes)
		for i := 0; i < numParamTypes; i++ {
			start := offset + (i * 4)
			state.unnamedParamOIDs[i] = binary.BigEndian.Uint32(payload[start : start+4])
		}
	}
	_ = writeParseComplete(conn)
}

func (s *Server) handleBind(conn net.Conn, state *extendedState, payload []byte) {
	offset := 0
	portalName, ok := readCString(payload, &offset)
	if !ok {
		_ = writePGError(conn, "invalid Bind message")
		return
	}
	statementName, ok := readCString(payload, &offset)
	if !ok {
		_ = writePGError(conn, "invalid Bind message")
		return
	}
	if portalName != "" || statementName != "" {
		_ = writePGError(conn, "only unnamed portal and statement are supported")
		return
	}
	if state.unnamedStatement == "" {
		_ = writePGError(conn, "no prepared statement")
		return
	}
	if len(payload[offset:]) < 2 {
		_ = writePGError(conn, "invalid Bind format codes")
		return
	}
	numFormatCodes := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if len(payload[offset:]) < numFormatCodes*2 {
		_ = writePGError(conn, "invalid Bind format code list")
		return
	}
	formatCodesOffset := offset
	offset += numFormatCodes * 2

	if len(payload[offset:]) < 2 {
		_ = writePGError(conn, "invalid Bind parameter count")
		return
	}
	numParams := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2

	formatCodes := make([]int16, numFormatCodes)
	if numFormatCodes > 0 {
		for i := 0; i < numFormatCodes; i++ {
			start := formatCodesOffset + (i * 2)
			formatCodes[i] = int16(binary.BigEndian.Uint16(payload[start : start+2]))
		}
	}

	params, err := decodeBindParams(payload, &offset, numParams, formatCodes, state.unnamedParamOIDs)
	if err != nil {
		_ = writePGError(conn, err.Error())
		return
	}

	if len(payload[offset:]) < 2 {
		_ = writePGError(conn, "invalid Bind result format count")
		return
	}
	numResultFormats := int(binary.BigEndian.Uint16(payload[offset : offset+2]))
	offset += 2
	if len(payload[offset:]) < numResultFormats*2 {
		_ = writePGError(conn, "invalid Bind result format list")
		return
	}

	state.unnamedPortal = state.unnamedStatement
	state.unnamedParams = params
	_ = writeBindComplete(conn)
}

func (s *Server) handleDescribe(conn net.Conn, state *extendedState, payload []byte) {
	if len(payload) < 1 {
		_ = writePGError(conn, "invalid Describe message")
		return
	}
	offset := 1
	name, ok := readCString(payload, &offset)
	if !ok {
		_ = writePGError(conn, "invalid Describe message")
		return
	}
	if name != "" {
		_ = writePGError(conn, "only unnamed statement and portal are supported")
		return
	}

	switch payload[0] {
	case 'S':
		if state.unnamedStatement == "" {
			_ = writePGError(conn, "no prepared statement")
			return
		}
		_ = writeParameterDescription(conn, state.unnamedParamOIDs)
		_ = writeNoData(conn)
	case 'P':
		if state.unnamedPortal == "" {
			_ = writePGError(conn, "no bound portal")
			return
		}
		_ = writeNoData(conn)
	default:
		_ = writePGError(conn, "unsupported Describe target")
	}
}

func (s *Server) handleExecute(conn net.Conn, state *extendedState, principal string, payload []byte, key backendKey) {
	offset := 0
	portalName, ok := readCString(payload, &offset)
	if !ok {
		_ = writePGError(conn, "invalid Execute message")
		return
	}
	if portalName != "" {
		_ = writePGError(conn, "only unnamed portal is supported")
		return
	}
	if len(payload[offset:]) < 4 {
		_ = writePGError(conn, "invalid Execute max rows")
		return
	}
	if state.unnamedPortal == "" {
		_ = writePGError(conn, "no bound portal")
		return
	}

	query, err := substituteBindParams(state.unnamedPortal, state.unnamedParams)
	if err != nil {
		_ = writePGError(conn, err.Error())
		return
	}

	queryCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.trackActiveQuery(key, cancel)
	result, err := s.query(queryCtx, principal, query)
	s.untrackActiveQuery(key)
	if err != nil {
		_ = writePGQueryError(conn, err)
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
	_ = writeCommandComplete(conn, fmt.Sprintf("SELECT %d", len(result.Rows)))
}

func (s *Server) handleClose(conn net.Conn, state *extendedState, payload []byte) {
	if len(payload) < 1 {
		_ = writePGError(conn, "invalid Close message")
		return
	}
	offset := 1
	name, ok := readCString(payload, &offset)
	if !ok {
		_ = writePGError(conn, "invalid Close message")
		return
	}
	if name != "" {
		_ = writePGError(conn, "only unnamed statement and portal are supported")
		return
	}

	switch payload[0] {
	case 'S':
		state.unnamedStatement = ""
		state.unnamedParams = nil
		state.unnamedParamOIDs = nil
	case 'P':
		state.unnamedPortal = ""
		state.unnamedParams = nil
	default:
		_ = writePGError(conn, "unsupported Close target")
		return
	}
	_ = writeCloseComplete(conn)
}

func (s *Server) handleSimpleQuery(conn net.Conn, principal string, payload []byte, key backendKey) {
	query := string(bytes.TrimSuffix(payload, []byte{0}))
	queryCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.trackActiveQuery(key, cancel)
	result, err := s.query(queryCtx, principal, query)
	s.untrackActiveQuery(key)
	if err != nil {
		_ = writePGQueryError(conn, err)
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

func (s *Server) trackActiveQuery(key backendKey, cancel context.CancelFunc) {
	s.queryMu.Lock()
	defer s.queryMu.Unlock()
	s.activeQueries[cancelKey{processID: key.processID, secretKey: key.secretKey}] = cancel
}

func (s *Server) untrackActiveQuery(key backendKey) {
	s.queryMu.Lock()
	defer s.queryMu.Unlock()
	delete(s.activeQueries, cancelKey{processID: key.processID, secretKey: key.secretKey})
}

func (s *Server) cancelQuery(processID, secretKey uint32) {
	s.queryMu.Lock()
	cancel := s.activeQueries[cancelKey{processID: int32(processID), secretKey: int32(secretKey)}]
	s.queryMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func newBackendKey() backendKey {
	return backendKey{processID: randomInt32(), secretKey: randomInt32()}
}

func randomInt32() int32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 1
	}
	v := int32(binary.BigEndian.Uint32(b[:]))
	if v == 0 {
		return 1
	}
	return v
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
	return writePGErrorCode(conn, message, "0A000")
}

func writePGQueryError(conn net.Conn, err error) error {
	if err == nil {
		return writePGError(conn, "query failed")
	}
	return writePGErrorCode(conn, err.Error(), sqlStateForError(err))
}

func writePGErrorCode(conn net.Conn, message, code string) error {
	body := make([]byte, 0, 128)
	body = append(body, 'S')
	body = append(body, []byte("ERROR")...)
	body = append(body, 0)
	body = append(body, 'C')
	body = append(body, []byte(code)...)
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

func sqlStateForError(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "57014"
	}

	var accessDenied *domain.AccessDeniedError
	if errors.As(err, &accessDenied) {
		return "42501"
	}
	var notFound *domain.NotFoundError
	if errors.As(err, &notFound) {
		return "42704"
	}
	var validation *domain.ValidationError
	if errors.As(err, &validation) {
		return "22023"
	}
	var conflict *domain.ConflictError
	if errors.As(err, &conflict) {
		return "23505"
	}
	var notImplemented *domain.NotImplementedError
	if errors.As(err, &notImplemented) {
		return "0A000"
	}

	return "XX000"
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

func writeBackendKeyData(conn net.Conn, processID, secretKey int32) error {
	packet := make([]byte, 1+4+8)
	packet[0] = 'K'
	binary.BigEndian.PutUint32(packet[1:5], 12)
	binary.BigEndian.PutUint32(packet[5:9], uint32(processID))
	binary.BigEndian.PutUint32(packet[9:13], uint32(secretKey))
	_, err := conn.Write(packet)
	return err
}

func writeParseComplete(conn net.Conn) error {
	packet := []byte{'1', 0, 0, 0, 4}
	_, err := conn.Write(packet)
	return err
}

func writeBindComplete(conn net.Conn) error {
	packet := []byte{'2', 0, 0, 0, 4}
	_, err := conn.Write(packet)
	return err
}

func writeCloseComplete(conn net.Conn) error {
	packet := []byte{'3', 0, 0, 0, 4}
	_, err := conn.Write(packet)
	return err
}

func writeNoData(conn net.Conn) error {
	packet := []byte{'n', 0, 0, 0, 4}
	_, err := conn.Write(packet)
	return err
}

func writeParameterDescription(conn net.Conn, paramOIDs []uint32) error {
	body := make([]byte, 2)
	binary.BigEndian.PutUint16(body[0:2], uint16(len(paramOIDs)))
	for _, oid := range paramOIDs {
		buf := make([]byte, 4)
		binary.BigEndian.PutUint32(buf, oid)
		body = append(body, buf...)
	}

	packet := make([]byte, 1+4+len(body))
	packet[0] = 't'
	binary.BigEndian.PutUint32(packet[1:5], uint32(4+len(body)))
	copy(packet[5:], body)
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

func readCString(payload []byte, offset *int) (string, bool) {
	start := *offset
	for i := start; i < len(payload); i++ {
		if payload[i] == 0 {
			*offset = i + 1
			return string(payload[start:i]), true
		}
	}
	return "", false
}

func decodeBindParams(payload []byte, offset *int, numParams int, formatCodes []int16, paramOIDs []uint32) ([]string, error) {
	out := make([]string, 0, numParams)
	for i := 0; i < numParams; i++ {
		formatCode := int16(0)
		switch len(formatCodes) {
		case 0:
			formatCode = 0
		case 1:
			formatCode = formatCodes[0]
		default:
			if i >= len(formatCodes) {
				return nil, fmt.Errorf("invalid Bind format code index")
			}
			formatCode = formatCodes[i]
		}

		if len(payload[*offset:]) < 4 {
			return nil, fmt.Errorf("invalid Bind parameter length")
		}
		length := int32(binary.BigEndian.Uint32(payload[*offset : *offset+4]))
		*offset += 4

		if length == -1 {
			out = append(out, "NULL")
			continue
		}
		if length < 0 || len(payload[*offset:]) < int(length) {
			return nil, fmt.Errorf("invalid Bind parameter payload")
		}
		raw := payload[*offset : *offset+int(length)]
		*offset += int(length)

		paramOID := uint32(0)
		if i < len(paramOIDs) {
			paramOID = paramOIDs[i]
		}

		sqlValue, err := decodeBindParamValue(formatCode, paramOID, raw)
		if err != nil {
			return nil, err
		}
		out = append(out, sqlValue)
	}
	return out, nil
}

func decodeBindParamValue(formatCode int16, paramOID uint32, raw []byte) (string, error) {
	switch formatCode {
	case 0:
		return quoteSQLLiteral(string(raw)), nil
	case 1:
		return decodeBinaryBindValue(paramOID, raw)
	default:
		return "", fmt.Errorf("unsupported Bind format code %d", formatCode)
	}
}

func decodeBinaryBindValue(paramOID uint32, raw []byte) (string, error) {
	switch paramOID {
	case 16: // BOOL
		if len(raw) != 1 {
			return "", fmt.Errorf("invalid binary bool parameter length")
		}
		if raw[0] == 0 {
			return "FALSE", nil
		}
		return "TRUE", nil
	case 20: // INT8
		if len(raw) != 8 {
			return "", fmt.Errorf("invalid binary int8 parameter length")
		}
		return strconv.FormatInt(int64(binary.BigEndian.Uint64(raw)), 10), nil
	case 21: // INT2
		if len(raw) != 2 {
			return "", fmt.Errorf("invalid binary int2 parameter length")
		}
		return strconv.FormatInt(int64(int16(binary.BigEndian.Uint16(raw))), 10), nil
	case 23: // INT4
		if len(raw) != 4 {
			return "", fmt.Errorf("invalid binary int4 parameter length")
		}
		return strconv.FormatInt(int64(int32(binary.BigEndian.Uint32(raw))), 10), nil
	case 700: // FLOAT4
		if len(raw) != 4 {
			return "", fmt.Errorf("invalid binary float4 parameter length")
		}
		v := math.Float32frombits(binary.BigEndian.Uint32(raw))
		return strconv.FormatFloat(float64(v), 'g', -1, 32), nil
	case 701: // FLOAT8
		if len(raw) != 8 {
			return "", fmt.Errorf("invalid binary float8 parameter length")
		}
		v := math.Float64frombits(binary.BigEndian.Uint64(raw))
		return strconv.FormatFloat(v, 'g', -1, 64), nil
	case 1082: // DATE
		if len(raw) != 4 {
			return "", fmt.Errorf("invalid binary date parameter length")
		}
		days := int32(binary.BigEndian.Uint32(raw))
		date := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).AddDate(0, 0, int(days))
		return quoteSQLLiteral(date.Format("2006-01-02")) + "::DATE", nil
	case 1114: // TIMESTAMP
		if len(raw) != 8 {
			return "", fmt.Errorf("invalid binary timestamp parameter length")
		}
		micros := int64(binary.BigEndian.Uint64(raw))
		ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(micros) * time.Microsecond)
		return quoteSQLLiteral(ts.Format("2006-01-02 15:04:05.999999")) + "::TIMESTAMP", nil
	case 1184: // TIMESTAMPTZ
		if len(raw) != 8 {
			return "", fmt.Errorf("invalid binary timestamptz parameter length")
		}
		micros := int64(binary.BigEndian.Uint64(raw))
		ts := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(micros) * time.Microsecond)
		return quoteSQLLiteral(ts.Format(time.RFC3339Nano)) + "::TIMESTAMPTZ", nil
	case 1700: // NUMERIC
		value, err := decodeBinaryNumeric(raw)
		if err != nil {
			return "", err
		}
		return value, nil
	case 2950: // UUID
		uuidLiteral, err := decodeBinaryUUID(raw)
		if err != nil {
			return "", err
		}
		return quoteSQLLiteral(uuidLiteral) + "::UUID", nil
	case 18, 19, 25, 1043: // CHAR, NAME, TEXT, VARCHAR
		return quoteSQLLiteral(string(raw)), nil
	default:
		return "", fmt.Errorf("unsupported binary parameter type oid %d", paramOID)
	}
}

func decodeBinaryUUID(raw []byte) (string, error) {
	if len(raw) != 16 {
		return "", fmt.Errorf("invalid binary uuid parameter length")
	}
	hexValue := hex.EncodeToString(raw)
	return fmt.Sprintf("%s-%s-%s-%s-%s", hexValue[0:8], hexValue[8:12], hexValue[12:16], hexValue[16:20], hexValue[20:32]), nil
}

func decodeBinaryNumeric(raw []byte) (string, error) {
	if len(raw) < 8 || (len(raw)-8)%2 != 0 {
		return "", fmt.Errorf("invalid binary numeric payload")
	}

	ndigits := int(int16(binary.BigEndian.Uint16(raw[0:2])))
	weight := int(int16(binary.BigEndian.Uint16(raw[2:4])))
	sign := binary.BigEndian.Uint16(raw[4:6])
	dscale := int(int16(binary.BigEndian.Uint16(raw[6:8])))
	if ndigits < 0 || dscale < 0 || len(raw) != 8+(ndigits*2) {
		return "", fmt.Errorf("invalid binary numeric header")
	}

	if sign == 0xC000 {
		return "", fmt.Errorf("numeric NaN is not supported")
	}
	if sign != 0x0000 && sign != 0x4000 {
		return "", fmt.Errorf("unsupported binary numeric sign %d", sign)
	}

	digits := make([]int, ndigits)
	for i := 0; i < ndigits; i++ {
		d := int(int16(binary.BigEndian.Uint16(raw[8+(i*2) : 10+(i*2)])))
		if d < 0 || d >= 10000 {
			return "", fmt.Errorf("invalid binary numeric digit")
		}
		digits[i] = d
	}

	intGroupCount := weight + 1
	intPart := "0"
	if intGroupCount > 0 {
		parts := make([]string, intGroupCount)
		for i := 0; i < intGroupCount; i++ {
			d := 0
			if i < len(digits) {
				d = digits[i]
			}
			if i == 0 {
				parts[i] = strconv.Itoa(d)
			} else {
				parts[i] = fmt.Sprintf("%04d", d)
			}
		}
		intPart = strings.TrimLeft(strings.Join(parts, ""), "0")
		if intPart == "" {
			intPart = "0"
		}
	}

	fracGroups := make([]int, 0)
	if intGroupCount < 0 {
		fracGroups = append(fracGroups, make([]int, -intGroupCount)...)
		fracGroups = append(fracGroups, digits...)
	} else if intGroupCount < len(digits) {
		fracGroups = append(fracGroups, digits[intGroupCount:]...)
	}

	fracPart := ""
	if len(fracGroups) > 0 {
		fracParts := make([]string, len(fracGroups))
		for i := range fracGroups {
			fracParts[i] = fmt.Sprintf("%04d", fracGroups[i])
		}
		fracPart = strings.Join(fracParts, "")
	}

	if dscale == 0 {
		if sign == 0x4000 && intPart != "0" {
			return "-" + intPart, nil
		}
		return intPart, nil
	}

	if len(fracPart) < dscale {
		fracPart += strings.Repeat("0", dscale-len(fracPart))
	} else if len(fracPart) > dscale {
		fracPart = fracPart[:dscale]
	}

	value := intPart + "." + fracPart
	if sign == 0x4000 && value != "0" && value != "0."+strings.Repeat("0", dscale) {
		value = "-" + value
	}
	return value, nil
}

func substituteBindParams(query string, params []string) (string, error) {
	if len(params) == 0 {
		return query, nil
	}
	rendered := query
	for i := len(params); i >= 1; i-- {
		placeholder := "$" + strconv.Itoa(i)
		if !strings.Contains(rendered, placeholder) {
			return "", fmt.Errorf("missing placeholder %s", placeholder)
		}
		rendered = strings.ReplaceAll(rendered, placeholder, params[i-1])
	}
	return rendered, nil
}

func quoteSQLLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}
