package simplewarc

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	hdr := make(Header)
	key := "hello"
	expected := "world"
	hdr[key] = expected
	actual := hdr.Get(key)
	require.Equal(t, expected, actual)
}

func TestSet(t *testing.T) {
	hdr := make(Header)
	key := "hello"
	expected := "world"
	hdr.Set(key, expected)
	actual := hdr.Get(key)
	require.Equal(t, expected, actual)
}

func TestDelete(t *testing.T) {
	hdr := make(Header)
	key := "hello"
	val := "world"
	hdr.Set(key, val)
	require.True(t, hdr.Has(key))
	hdr.Delete(key)
	require.False(t, hdr.Has(key))
}

func TestHas(t *testing.T) {
	hdr := make(Header)
	key := "hello"
	val := "world"
	hdr.Set(key, val)
	require.True(t, hdr.Has(key))
}

func TestContentLength(t *testing.T) {
	hdr := make(Header)
	key := "content-length"
	val := "1234"
	hdr.Set(key, val)
	expected := int64(1234)
	actual := hdr.ContentLength()
	require.Equal(t, expected, actual)
}

func TestSplitLine(t *testing.T) {
	key := "hello"
	val := "world"
	delim := ":"
	line := fmt.Sprintf("%s%s%s", key, delim, val)
	actualKey, actualVal := splitLine(line, delim)
	require.Equal(t, key, actualKey)
	require.Equal(t, val, actualVal)
}
