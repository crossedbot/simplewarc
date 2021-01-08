package simplewarc

import (
	"bufio"
	"crypto/sha1"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRead(t *testing.T) {
	fd, err := os.Open("./testdata/hello.warc.gz")
	require.Nil(t, err)
	reader, err := New(fd)
	require.Nil(t, err)
	expected := []byte("WARC/1.0")
	actual := make([]byte, len(expected))
	length, err := reader.Read(actual)
	require.Nil(t, err)
	require.Equal(t, len(expected), length)
	require.Equal(t, expected, actual)
}

func testNext(t *testing.T, reader Reader) {
	record, err := reader.Next()
	require.Nil(t, err)
	contentLength := record.Header.ContentLength()
	require.Greater(t, contentLength, int64(0))
	limitReader := io.LimitReader(record.Content, contentLength)
	contents, err := ioutil.ReadAll(limitReader)
	require.Nil(t, err)
	require.NotEqual(t, "", string(contents))
}

func TestNext(t *testing.T) {
	// Default (Source) and GZIP
	fd, err := os.Open("./testdata/hello.warc.gz")
	require.Nil(t, err)
	reader, err := New(fd)
	require.Nil(t, err)
	// Test for 2 records... well, there should be 2!
	testNext(t, reader)
	testNext(t, reader)
	reader.Close()
	fd.Close()

	// Copy and BZIP2
	fd, err = os.Open("./testdata/hello.warc.bz2")
	require.Nil(t, err)
	reader, err = New(fd)
	require.Nil(t, err)
	// Test for 2 records... well, there should be 2!
	testNext(t, reader)
	testNext(t, reader)
}

func TestSeek(t *testing.T) {
	fd, err := os.Open("./testdata/hello.warc.gz")
	require.Nil(t, err)
	reader, err := New(fd)
	require.Nil(t, err)
	for i := 0; i < 2; i++ {
		err := reader.Seek()
		require.Nil(t, err)
	}
}

func TestReadLine(t *testing.T) {
	fd, err := os.Open("./testdata/hello.warc.gz")
	require.Nil(t, err)
	reader, err := New(fd)
	require.Nil(t, err)
	version, err := reader.ReadLine()
	require.Nil(t, err)
	require.Equal(t, "WARC/1.0", version)
}

func TestHeader(t *testing.T) {
	expected := Header{
		"warc-date":         "2009-11-10T23:12:00+01:00",
		"warc-type":         "resource",
		"content-type":      "text/plain",
		"warc-block-digest": "sha1:0abcd9a9d15f8fa64d19c17fdc752fcc08671fc5",
		"content-length":    "2054",
	}
	fd, err := os.Open("./testdata/hello.warc.gz")
	require.Nil(t, err)
	decompressed, err := decompress(fd)
	require.Nil(t, err)
	reader := &reader{
		Source: decompressed,
		Reader: bufio.NewReader(decompressed),
		Record: nil,
	}
	require.Nil(t, err)
	actual, n, err := reader.Header()
	require.Nil(t, err)
	require.NotZero(t, n)
	for key, expectedValue := range expected {
		require.True(t, actual.Has(key))
		actualValue := actual.Get(key)
		require.Equal(t, expectedValue, actualValue)
	}
}

func TestContent(t *testing.T) {
	fd, err := os.Open("./testdata/hello.warc.gz")
	require.Nil(t, err)
	decompressed, err := decompress(fd)
	require.Nil(t, err)
	reader := &reader{
		Source: decompressed,
		Reader: bufio.NewReader(decompressed),
		Record: nil,
	}
	require.Nil(t, err)
	hdr, n, err := reader.Header()
	require.Nil(t, err)
	require.NotZero(t, n)
	expected := [20]byte{
		0x0a, 0xbc, 0xd9, 0xa9, 0xd1, 0x5f, 0x8f, 0xa6,
		0x4d, 0x19, 0xc1, 0x7f, 0xdc, 0x75, 0x2f, 0xcc,
		0x08, 0x67, 0x1f, 0xc5,
	}
	content := reader.Content(hdr.ContentLength())
	b, err := ioutil.ReadAll(content)
	require.Nil(t, err)
	actual := sha1.Sum(b)
	require.Equal(t, expected, actual)
}
