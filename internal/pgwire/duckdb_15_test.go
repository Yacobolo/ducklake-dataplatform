package pgwire

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDuckDB15_DescribeResultColumns_PythonLambdaQuery(t *testing.T) {
	cols := describeResultColumns(`SELECT list_transform([1, 2, 3], lambda x: x + 1) AS transformed`)
	assert.Equal(t, []string{"transformed"}, cols)
}
