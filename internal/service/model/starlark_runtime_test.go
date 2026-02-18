package model

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStarlarkRuntime_ModuleSizeLimit(t *testing.T) {
	_, err := newStarlarkMacroRuntimeFromModules(map[string]string{
		"utils": "#" + strings.Repeat("x", maxStarlarkModuleBytes+1),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds")
}

func TestStarlarkRuntime_ArgumentSizeLimit(t *testing.T) {
	runtime, err := newStarlarkMacroRuntimeFromModules(map[string]string{
		"utils": "def echo(x):\n    return x\n",
	})
	require.NoError(t, err)
	runtime.maxArgBytes = 8

	_, err = runtime.EvalMacro(compileMacroDefinition{name: "utils.echo", starlark: true}, []string{"'123456789'"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "arguments exceed")
}

func TestStarlarkRuntime_EvalTimeout(t *testing.T) {
	runtime, err := newStarlarkMacroRuntimeFromModules(map[string]string{
		"utils": "def spin():\n    total = 0\n    for i in range(0, 1000000000):\n        total += i\n    return str(total)\n",
	})
	require.NoError(t, err)
	runtime.maxSteps = 1_000_000_000
	runtime.evalTimeout = 5 * time.Millisecond

	_, err = runtime.EvalMacro(compileMacroDefinition{name: "utils.spin", starlark: true}, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "timed out")
}
