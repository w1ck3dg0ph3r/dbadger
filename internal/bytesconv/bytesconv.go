package bytesconv

import (
	"reflect"
	"unsafe"
)

// ToString converts byte slice to a string without memory allocation.
// See https://groups.google.com/forum/#!msg/Golang-Nuts/ENgbUzYvCuU/90yGx7GUAgAJ .
func ToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// ToBytes converts string to a byte slice without memory allocation.
//
// Note it may break if string and/or slice header will change
// in the future go versions.
func ToBytes(s string) (b []byte) {
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len

	return b
}

// ToStringSlice converts [][]byte to []string using [ToString].
func ToStringSlice(bb [][]byte) (ss []string) {
	ss = make([]string, len(bb))
	for i := range bb {
		ss[i] = ToString(bb[i])
	}
	return ss
}

// ToBytesSlice converts []string to [][]byte using [ToBytes].
func ToBytesSlice(ss []string) (bb [][]byte) {
	bb = make([][]byte, len(ss))
	for i := range ss {
		bb[i] = ToBytes(ss[i])
	}
	return bb
}
