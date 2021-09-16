package bytesconv

import (
	"bytes"
	"testing"
)

func Test_ToString(t *testing.T) {
	t.Parallel()
	b := []byte("test")
	s := ToString(b)
	if s != string(b) {
		t.Errorf(`b2s: "%s" != "%s"`, s, string(b))
	}
}

func Test_ToBytes(t *testing.T) {
	t.Parallel()
	s := "test"
	b := ToBytes(s)
	if !bytes.Equal(b, []byte(s)) {
		t.Errorf(`s2b: "%s" != "%s"`, string(b), s)
	}
}

func Test_ToStringSlice(t *testing.T) {
	t.Parallel()
	bb := [][]byte{[]byte("one"), []byte("two"), []byte("three")}
	ss := ToStringSlice(bb)
	for i := range bb {
		if !bytes.Equal(bb[i], []byte(ss[i])) {
			t.Errorf(`bb2ss: "%s" != "%s"`, ss[i], string(bb[i]))
		}
	}
}

func Test_ToBytesSlice(t *testing.T) {
	t.Parallel()
	ss := []string{"one", "two", "three"}
	bb := ToBytesSlice(ss)
	for i := range ss {
		if ss[i] != string(bb[i]) {
			t.Errorf(`ss2bb: "%s" != "%s"`, string(bb[i]), ss[i])
		}
	}
}
