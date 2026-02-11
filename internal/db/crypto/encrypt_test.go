package crypto

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryptor_RoundTrip(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	enc, err := NewEncryptor(key)
	require.NoError(t, err)

	tests := []struct {
		name      string
		plaintext string
	}{
		{"empty string", ""},
		{"short secret", "my-secret-key-id"},
		{"long secret", "a-very-long-secret-access-key-that-has-many-characters-1234567890"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ciphertext, err := enc.Encrypt(tt.plaintext)
			require.NoError(t, err)
			assert.NotEqual(t, tt.plaintext, ciphertext)

			decrypted, err := enc.Decrypt(ciphertext)
			require.NoError(t, err)
			assert.Equal(t, tt.plaintext, decrypted)
		})
	}
}

func TestEncryptor_DifferentCiphertexts(t *testing.T) {
	key := "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	enc, err := NewEncryptor(key)
	require.NoError(t, err)

	c1, err := enc.Encrypt("same-text")
	require.NoError(t, err)
	c2, err := enc.Encrypt("same-text")
	require.NoError(t, err)

	assert.NotEqual(t, c1, c2, "encrypting the same plaintext should produce different ciphertexts (different nonces)")
}

func TestEncryptor_InvalidKey(t *testing.T) {
	_, err := NewEncryptor("tooshort")
	require.Error(t, err)
}

func TestEncryptor_InvalidHex(t *testing.T) {
	_, err := NewEncryptor("zzzz")
	require.Error(t, err)
}
