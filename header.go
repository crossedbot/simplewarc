package simplewarc

import (
	"strconv"
	"strings"
)

// Header represents a WARC named-field
type Header map[string]string

// Get returns the field-value set for the field-name
func (h Header) Get(key string) string {
	if h == nil {
		return ""
	}
	return h[key]
}

// Set sets the field-value for the given field-name
func (h Header) Set(key, val string) {
	h[key] = val
}

// Delete removes the named-field for the given field-name
func (h Header) Delete(key string) {
	delete(h, key)
}

// Has returns true if the field-name exists as a named-field
func (h Header) Has(key string) bool {
	_, ok := h[key]
	return ok
}

// ContentLength returns the content-length field-value
func (h Header) ContentLength() int64 {
	v := h.Get("content-length")
	if v != "" {
		i, _ := strconv.ParseInt(v, 10, 64)
		return i
	}
	return 0
}

// splitLine splits a given line for the given delimiter
func splitLine(line, delim string) (key string, val string) {
	parts := strings.Split(line, delim)
	key = parts[0]
	if len(parts) > 1 {
		val = strings.Join(parts[1:], delim)
	}
	key = strings.TrimSpace(key)
	val = strings.TrimSpace(val)
	return
}
