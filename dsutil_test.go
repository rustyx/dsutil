package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseFilter(t *testing.T) {
	require.True(t, simpleFilter(`Aaaa12_2`))
	require.False(t, simpleFilter(`AAA bbb`))
	require.False(t, simpleFilter(``))
	require.False(t, simpleFilter(`Abc =2`))
	require.False(t, simpleFilter(`Abc=2`))
	require.False(t, simpleFilter(`Abc!=2`))
	require.False(t, simpleFilter(`Abc>=2`))
	require.False(t, simpleFilter(`Abc<=2`))
	require.False(t, simpleFilter(`Abc<2`))
	require.False(t, simpleFilter(`Abc>2`))
	require.False(t, simpleFilter(`Abc>>2`))
	require.False(t, simpleFilter(`Abc!2`))
}
