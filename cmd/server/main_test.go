package main

import (
	"flag"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCurlHostForListenAddr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		listenAddr string
		want       string
	}{
		{name: "port only", listenAddr: ":8080", want: "localhost:8080"},
		{name: "ipv4 host and port", listenAddr: "127.0.0.1:8080", want: "127.0.0.1:8080"},
		{name: "wildcard ipv4", listenAddr: "0.0.0.0:8080", want: "localhost:8080"},
		{name: "wildcard ipv6", listenAddr: "[::]:8080", want: "localhost:8080"},
		{name: "ipv6 loopback", listenAddr: "[::1]:8080", want: "[::1]:8080"},
		{name: "trim host and port", listenAddr: " localhost:9090 ", want: "localhost:9090"},
		{name: "trim port only", listenAddr: "  :7070  ", want: "localhost:7070"},
		{name: "empty falls back", listenAddr: "", want: "localhost:8080"},
		{name: "whitespace falls back", listenAddr: "   ", want: "localhost:8080"},
		{name: "malformed passes through", listenAddr: "localhost", want: "localhost"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := curlHostForListenAddr(tt.listenAddr)

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestWantsServerHelp(t *testing.T) {
	t.Parallel()

	assert.True(t, wantsServerHelp([]string{"--help"}))
	assert.True(t, wantsServerHelp([]string{"-h"}))
	assert.True(t, wantsServerHelp([]string{"help"}))
	assert.False(t, wantsServerHelp(nil))
	assert.False(t, wantsServerHelp([]string{"admin"}))
}

func TestRunAdmin_Help(t *testing.T) {
	t.Parallel()

	err := runAdmin([]string{"--help"})
	require.ErrorIs(t, err, flag.ErrHelp)
}
