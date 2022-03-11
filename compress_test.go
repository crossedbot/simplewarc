package simplewarc

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/require"
)

func testCompressionType(t *testing.T, compressed []byte, expected CompressionType) {
	reader := bufio.NewReader(bytes.NewReader(compressed))
	actual, err := compressionType(reader)
	require.Nil(t, err)
	require.Equal(t, expected, actual)
}

func TestCompressionType(t *testing.T) {
	// test for gzip
	gzipContents, err := ioutil.ReadFile("./testdata/hello.txt.gz")
	require.Nil(t, err)
	testCompressionType(t, gzipContents, GzipCompression)
	// test for bzip2
	bzipContents, err := ioutil.ReadFile("./testdata/hello.txt.bz2")
	require.Nil(t, err)
	testCompressionType(t, bzipContents, BzipCompression)
}

func testDecompress(t *testing.T, compressed, expected []byte) {
	readCloser, err := decompress(bytes.NewReader(compressed))
	require.Nil(t, err)
	actual, err := ioutil.ReadAll(readCloser)
	require.Nil(t, err)
	require.Equal(t, expected, actual)
	readCloser.Close()
}

func TestDecompress(t *testing.T) {
	expected := []byte("Hello World")
	// test for gzip
	gzipContents, err := ioutil.ReadFile("./testdata/hello.txt.gz")
	require.Nil(t, err)
	testDecompress(t, gzipContents, expected)
	// test for bzip2
	bzipContents, err := ioutil.ReadFile("./testdata/hello.txt.bz2")
	require.Nil(t, err)
	testDecompress(t, bzipContents, expected)
}
