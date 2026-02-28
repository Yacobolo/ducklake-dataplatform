package cli

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateOutputFormat(t *testing.T) {
	tests := []struct {
		name    string
		output  string
		wantErr bool
	}{
		{name: "empty ok", output: "", wantErr: false},
		{name: "table ok", output: "table", wantErr: false},
		{name: "json ok", output: "json", wantErr: false},
		{name: "yaml rejected", output: "yaml", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateOutputFormat(tt.output)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
