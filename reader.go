package simplewarc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
)

const (
	ChunkSize                 = 4096
	RecordDelimitingLineCount = 2
)

// Record represents a WARC record
type Record struct {
	Header  Header
	Content io.Reader
}

// Reader represents a WARC archive reader
type Reader interface {
	// Close closes the reader
	Close() error

	// Read reads up up to len(p) bytes into p
	Read(p []byte) (n int, err error)

	// Next returns the next WARC record in the archive
	Next() (*Record, error)

	// Seek sets the reader to the next record in the archive
	Seek() error

	// ReadLine reads the next line in the current record
	ReadLine() (string, error)
}

// reader represents an implementation of a WARC archive reader
type reader struct {
	Source io.ReadCloser // Decompressed source reader
	Reader *bufio.Reader // Reader used for reading by mode
	Record *Record       // The current record being tracked
}

// New wraps the given reader and returns a new WARC reader
func New(source io.Reader) (Reader, error) {
	r, err := decompress(source)
	if err != nil {
		return nil, err
	}
	return &reader{
		Source: r,
		Reader: bufio.NewReader(r),
		Record: nil,
	}, nil
}

// Close closes the reader
func (r *reader) Close() error {
	if r != nil {
		if err := r.Source.Close(); err != nil {
			return err
		}
		r.Source = nil
		r.Reader = nil
		r.Record = nil
	}
	return nil
}

func (r *reader) Read(p []byte) (int, error) {
	return r.Reader.Read(p)
}

// Next returns the next WARC record in the archive
func (r *reader) Next() (*Record, error) {
	// move to next record
	if err := r.Seek(); err != nil {
		return nil, err
	}
	// skip version line
	if _, err := r.ReadLine(); err != nil {
		return nil, err
	}
	// build the header
	header, err := r.Header()
	if err != nil {
		return nil, err
	}
	// parse the contents based on the content-length header
	content := r.Content(header.ContentLength())
	r.Record = &Record{Header: header, Content: content}
	return r.Record, nil
}

// Seek sets the reader to the next record in the archive
func (r *reader) Seek() error {
	// if no record, we are at the top, and should return
	if r.Record == nil {
		return nil
	}
	// read all lines until we reach the end
	tmp := make([]byte, ChunkSize)
	n, err := r.Record.Content.Read(tmp)
	for n != 0 || err == nil {
		n, err = r.Record.Content.Read(tmp)
	}
	// then reset the tracked record
	r.Record = nil
	// and read in the two CRLF delimiting lines at the end of a record
	for i := 0; i < RecordDelimitingLineCount; i++ {
		line, err := r.ReadLine()
		if err != nil {
			return err
		}
		if line != "" {
			return fmt.Errorf("unexpected line at end of record: %s", line)
		}
	}
	return nil
}

// ReadLine reads the next line in the current record
func (r *reader) ReadLine() (string, error) {
	// read in the next line from the source
	line, isPrefix, err := r.Reader.ReadLine()
	if err != nil {
		return "", err
	}
	str := string(line)
	// if the line exceeds the buffer, get the rest and return the built line
	if isPrefix {
		buffer := bytes.NewBuffer(line)
		for isPrefix {
			line, isPrefix, err = r.Reader.ReadLine()
			if err != nil {
				return "", err
			}
			buffer.Write(line)
		}
		str = buffer.String()
	}
	return str, nil
}

// Header returns the header of the current WARC record
func (r *reader) Header() (Header, error) {
	header := make(Header)
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	for line != "" {
		if key, value := splitLine(line, ":"); key != "" {
			key = strings.ToLower(key) // normalize to lowercase
			header[key] = value
		}
		line, err = r.ReadLine()
		if err != nil {
			return nil, err
		}
	}
	return header, nil
}

// Content returns the content of the current WARC record
func (r *reader) Content(length int64) io.Reader {
	return io.LimitReader(r.Reader, length)
}
