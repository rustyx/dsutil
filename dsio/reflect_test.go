package dsio

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

type A struct {
	P    int
	Q    int32
	R    float64
	S    float32
	X, Y string
	Z    bool
	U    uint64
	T    *B
}

type B struct {
	X string
}

func TestReflector(t *testing.T) {
	r := NewReflector(&A{})
	r.Reset()
	r.Set("P", 1)
	r.Set("P", int8(1))
	r.Set("P", int16(1))
	r.Set("P", int32(1))
	r.Set("P", int64(1))
	r.Set("Q", int32(2))
	r.Set("R", 2.25)
	r.Set("S", 2.25)
	r.Set("S", float32(2.125))
	r.Set("X", "xxx")
	r.Set("Z", true)
	r.Set("U", uint64(5))
	r.Set("U", uint32(4))
	r.Set("U", uint16(3))
	r.Set("U", uint8(2))
	r.Set("Eh", 2)
	r.Set("T", &B{"x"})
	a := r.MakeCopy().(*A)
	require.Equal(t, 1, a.P)
	require.Equal(t, int32(2), a.Q)
	require.Equal(t, 2.25, a.R)
	require.Equal(t, float32(2.125), a.S)
	require.Equal(t, "xxx", a.X)
	require.Equal(t, "", a.Y)
	require.Equal(t, true, a.Z)
	require.Equal(t, uint64(2), a.U)
	require.Equal(t, "x", a.T.X)
	r.Reset()
	b := r.MakeCopy().(*A)
	require.EqualValues(t, A{}, *b)
	require.Equal(t, "xxx", a.X) // should not be touched
}

func BenchmarkBaseline(b *testing.B) {
	ch := make(chan interface{}, 128)
	defer close(ch)
	go sink(ch)
	for i := 0; i < b.N; i++ {
		tmp := &A{}
		tmp.X = "xxx"
		tmp.Y = "yyy"
		tmp.P = i
		tmp.Q = int32(i)
		tmp.R = float64(i)
		tmp.S = float32(i)
		ch <- tmp
	}
}

func BenchmarkReflect(b *testing.B) {
	ch := make(chan interface{}, 128)
	defer close(ch)
	go sink(ch)
	t := reflect.ValueOf(&A{}).Elem()
	for i := 0; i < b.N; i++ {
		aptr := reflect.New(t.Type())
		a := aptr.Elem()
		a.FieldByName("X").SetString("xxx")
		a.FieldByName("Y").SetString("yyy")
		a.FieldByName("P").SetInt(int64(i))
		a.FieldByName("Q").SetInt(int64(i))
		a.FieldByName("R").SetFloat(float64(i))
		a.FieldByName("S").SetFloat(float64(i))
		ch <- aptr
	}
}

func BenchmarkReflector(b *testing.B) {
	ch := make(chan interface{}, 128)
	defer close(ch)
	go sink(ch)
	r := NewReflector(&A{})
	for i := 0; i < b.N; i++ {
		r.Reset()
		r.Set("X", "xxx")
		r.Set("Y", "yyy")
		r.Set("P", i)
		r.Set("Q", int32(i))
		r.Set("R", float64(i))
		r.Set("S", float32(i))
		ch <- r.MakeCopy()
	}
}

// to prevent optimization of unused value
func sink(ch chan interface{}) {
	for range ch {
	}
}
