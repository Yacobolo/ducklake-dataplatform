package cli

import "testing"

func TestValidateHostURL(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		wantErr bool
	}{
		{name: "valid http", host: "http://127.0.0.1:8080"},
		{name: "valid https", host: "https://api.example.com"},
		{name: "missing scheme", host: "localhost:8080", wantErr: true},
		{name: "bogus scheme", host: "://bad", wantErr: true},
		{name: "empty", host: "", wantErr: true},
		{name: "path not allowed", host: "http://localhost:8080/v1", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateHostURL(tt.host)
			if tt.wantErr && err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("expected nil error, got %v", err)
			}
		})
	}
}
