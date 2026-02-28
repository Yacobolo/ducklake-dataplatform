package compute

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"
)

// Agent request headers used by control-plane <-> compute-agent auth.
const (
	HeaderAgentAuth      = "X-Agent-Token"
	HeaderAgentTimestamp = "X-Agent-Timestamp"
	HeaderAgentSignature = "X-Agent-Signature"
)

// AttachSignedAgentHeaders adds agent authentication and integrity headers.
func AttachSignedAgentHeaders(req *http.Request, token string, body []byte, now time.Time) {
	ts := strconv.FormatInt(now.UTC().Unix(), 10)
	req.Header.Set(HeaderAgentAuth, token)
	req.Header.Set(HeaderAgentTimestamp, ts)
	req.Header.Set(HeaderAgentSignature, signRequest(req.Method, req.URL.Path, ts, body, token))
}

// VerifySignedAgentHeaders validates timestamp freshness and request signature.
func VerifySignedAgentHeaders(req *http.Request, token string, body []byte, now time.Time, maxSkew time.Duration) error {
	tsRaw := req.Header.Get(HeaderAgentTimestamp)
	if tsRaw == "" {
		return fmt.Errorf("missing %s", HeaderAgentTimestamp)
	}
	timestamp, err := strconv.ParseInt(tsRaw, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid %s: %w", HeaderAgentTimestamp, err)
	}

	skew := now.UTC().Sub(time.Unix(timestamp, 0).UTC())
	if skew < 0 {
		skew = -skew
	}
	if skew > maxSkew {
		return fmt.Errorf("request timestamp outside allowed skew")
	}

	gotSig := req.Header.Get(HeaderAgentSignature)
	if gotSig == "" {
		return fmt.Errorf("missing %s", HeaderAgentSignature)
	}

	expected := signRequest(req.Method, req.URL.Path, tsRaw, body, token)
	if !hmac.Equal([]byte(gotSig), []byte(expected)) {
		return fmt.Errorf("invalid request signature")
	}

	return nil
}

func signRequest(method, path, ts string, body []byte, token string) string {
	bodyDigest := sha256.Sum256(body)
	payload := method + "\n" + path + "\n" + ts + "\n" + hex.EncodeToString(bodyDigest[:])

	mac := hmac.New(sha256.New, []byte(token))
	_, _ = mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}
