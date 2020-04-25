package dsio

import (
	"testing"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/require"
)

func TestMarshalKey_basic(t *testing.T) {
	key := datastore.IDKey("B", 22, datastore.NameKey("A", "x", nil))
	enc := MarshalKey(key)
	require.Equal(t, "/A,x/B,22", enc)
	require.Equal(t, key, UnmarshalKey(enc))
}

func TestMarshalKey_basic3(t *testing.T) {
	key := datastore.NameKey("CccCCc", "some value 123", datastore.NameKey("B", "B", datastore.NameKey("A", "A", nil)))
	enc := MarshalKey(key)
	require.Equal(t, "/A,A/B,B/CccCCc,some value 123", enc)
	require.Equal(t, key, UnmarshalKey(enc))
}

func TestMarshalKey_incomplete(t *testing.T) {
	key := datastore.IDKey("B", 0, datastore.NameKey("A", "x", nil))
	enc := MarshalKey(key)
	require.Equal(t, "/A,x/B,0", enc)
	require.Equal(t, key, UnmarshalKey(enc))
}

func TestMarshalKey_digit_name(t *testing.T) {
	key := datastore.NameKey("B", "22", datastore.IDKey("A", 0, nil))
	enc := MarshalKey(key)
	require.Equal(t, "/A,0/B,`22", enc)
	require.Equal(t, key, UnmarshalKey(enc))
}

func TestMarshalKey_escaping(t *testing.T) {
	key := datastore.NameKey("B", "`^^2^^`", datastore.IDKey("A", 0, nil))
	enc := MarshalKey(key)
	require.Equal(t, "/A,0/B,^`^^^^2^^^^^`", enc)
	require.Equal(t, key, UnmarshalKey(enc))
}

func TestMarshalKey_NS(t *testing.T) {
	key := datastore.NameKey("B", "`2`", datastore.IDKey("A", 0, nil))
	key.Namespace = "ns2^"
	key.Parent.Namespace = "ns1`"
	enc := MarshalKey(key)
	require.Equal(t, "/A,0`ns1^`/B,^`2^``ns2^^", enc)
	require.Equal(t, key, UnmarshalKey(enc))
}
