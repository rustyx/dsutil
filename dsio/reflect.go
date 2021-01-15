package dsio

import (
	"io"
	"os"
	"reflect"
)

// ImportFuncType is the type of the import callback.
type ImportFuncType func(rows []interface{}) error

// ImportFileReflect imports a given .ds file using the provided type and import function.
// typePtr must be a pointer to a struct of the desired type.
// importFunc will be called with a slice of pointers to objects of the given type.
// The slice will have at most batchSize elements.
func ImportFileReflect(filename string, typePtr interface{}, importFunc ImportFuncType, batchSize int) error {
	infile, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer infile.Close()
	return ImportStreamReflect(infile, typePtr, importFunc, batchSize)
}

// ImportStreamReflect imports a given .ds stream using the provided type and import function
// typePtr must be a pointer to a struct of the desired type.
// importFunc will be called with a slice of pointers to objects of the given type.
// The slice will have at most batchSize elements.
func ImportStreamReflect(r io.Reader, typePtr interface{}, importFunc ImportFuncType, batchSize int) (err error) {
	outCh := make(chan Entity, 10)
	errCh := make(chan error, 1)
	tmp := NewReflector(typePtr)
	go func() {
		defer close(errCh)
		var rows []interface{}
		for e := range outCh {
			tmp.Reset()
			for _, p := range e.Properties {
				tmp.Set(p.Name, p.Value)
			}
			rows = append(rows, tmp.MakeCopy())
			if len(rows) >= batchSize {
				err := importFunc(rows)
				if err != nil {
					errCh <- err
					return
				}
				rows = nil
			}
		}
		if len(rows) > 0 {
			err := importFunc(rows)
			if err != nil {
				errCh <- err
			}
		}
	}()
	err = ImportFile(r, outCh, errCh)
	return
}

// Reflector implements a caching reflection helper.
type Reflector struct {
	typ    reflect.Type
	tmpptr reflect.Value
	tmp    reflect.Value
	fields map[string]*reflect.Value
}

// NewReflector returns a new instance of Reflector.
// Typical use-case flow: r.Reset(), r.Set(), r.Set() ..., r.MakeCopy()
func NewReflector(typePtr interface{}) *Reflector {
	typ := reflect.TypeOf(typePtr).Elem()
	tmpptr := reflect.New(typ)
	return &Reflector{
		typ:    typ,
		tmpptr: tmpptr,
		tmp:    tmpptr.Elem(),
		fields: make(map[string]*reflect.Value),
	}
}

// Reset default-initializes the reflected object.
func (r *Reflector) Reset() {
	r.tmp.Set(reflect.Zero(r.typ))
}

// Set sets a property in the reflected object.
func (r *Reflector) Set(field string, value interface{}) {
	f, ok := r.fields[field]
	if !ok {
		ftmp := r.tmp.FieldByName(field)
		f = &ftmp
		r.fields[field] = f
	}
	v := reflect.ValueOf(value).Convert(f.Type())
	f.Set(v)
}

// MakeCopy returns a pointer to a copy of the reflected object.
func (r *Reflector) MakeCopy() interface{} {
	vptr := reflect.New(r.typ)
	vptr.Elem().Set(r.tmp)
	return vptr.Interface()
}
