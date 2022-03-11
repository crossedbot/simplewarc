package simplewarc

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
)

type CompressionType int

const (
	// Compression Types
	NoCompression CompressionType = iota + 1
	BzipCompression
	GzipCompression
)

// decompress attempts to decompress the contents of the reader
func decompress(r io.Reader) (io.ReadCloser, error) {
	bReader := bufio.NewReader(r)
	compress, err := compressionType(bReader)
	if err != nil {
		return nil, err
	}
	var readCloser io.ReadCloser
	switch compress {
	case GzipCompression:
		readCloser, err = gzip.NewReader(bReader)
		if err != nil {
			return nil, err
		}
	case BzipCompression:
		readCloser = ioutil.NopCloser(bzip2.NewReader(bReader))
	case NoCompression:
		readCloser = ioutil.NopCloser(bReader)
	default:
		// shouldn't get here but worth having
		return nil, fmt.Errorf("unknown compression type")
	}
	return readCloser, nil
}

// compressionType attempts to determine the compression type
func compressionType(r *bufio.Reader) (CompressionType, error) {
	b, err := r.Peek(2)
	if err != nil && err != io.EOF {
		return NoCompression, err
	}
	t := NoCompression
	if len(b) > 1 {
		switch {
		case b[0] == 0x42 && b[1] == 0x5a:
			t = BzipCompression
		case b[0] == 0x1f && b[1] == 0x8b:
			t = GzipCompression
		}
	}
	return t, nil
}
