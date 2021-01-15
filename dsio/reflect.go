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
	fields map[string]*refField
}

type refField struct {
	reflect.Value
	setValue setOp
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
		fields: make(map[string]*refField),
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
		ftmp := r.makeRefField(field)
		r.fields[field] = ftmp
		f = ftmp
	}
	f.setValue(&f.Value, value)
}

// MakeCopy returns a pointer to a copy of the reflected object.
func (r *Reflector) MakeCopy() interface{} {
	vptr := reflect.New(r.typ)
	vptr.Elem().Set(r.tmp)
	return vptr.Interface()
}

// setAny() can set any value, the rest of the code below is for performance only.

func (r *Reflector) makeRefField(name string) *refField {
	f := &refField{Value: r.tmp.FieldByName(name)}
	switch f.Kind() {
	case reflect.Bool:
		f.setValue = setBool
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		f.setValue = setInt
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		f.setValue = setUInt
	case reflect.Float32, reflect.Float64:
		f.setValue = setFloat
	case reflect.String:
		f.setValue = setString
	default:
		f.setValue = setAny
	}
	return f
}

type setOp func(f *reflect.Value, value interface{})

func setBool(f *reflect.Value, value interface{}) {
	if v, ok := value.(bool); ok {
		f.SetBool(v)
		return
	}
	v := reflect.ValueOf(value).Convert(f.Type())
	f.Set(v)
}

func setInt(f *reflect.Value, value interface{}) {
	if v, ok := value.(int); ok {
		f.SetInt(int64(v))
	} else if v, ok := value.(int64); ok {
		if v != 0 {
			f.SetInt(v)
		}
	} else if v, ok := value.(int32); ok {
		if v != 0 {
			f.SetInt(int64(v))
		}
	} else if v, ok := value.(int16); ok {
		if v != 0 {
			f.SetInt(int64(v))
		}
	} else if v, ok := value.(int8); ok {
		if v != 0 {
			f.SetInt(int64(v))
		}
	} else {
		v := reflect.ValueOf(value).Convert(f.Type())
		f.Set(v)
	}
}

func setUInt(f *reflect.Value, value interface{}) {
	if v, ok := value.(uint64); ok {
		if v != 0 {
			f.SetUint(v)
		}
	} else if v, ok := value.(uint32); ok {
		if v != 0 {
			f.SetUint(uint64(v))
		}
	} else if v, ok := value.(uint16); ok {
		if v != 0 {
			f.SetUint(uint64(v))
		}
	} else if v, ok := value.(uint8); ok {
		if v != 0 {
			f.SetUint(uint64(v))
		}
	} else {
		v := reflect.ValueOf(value).Convert(f.Type())
		f.Set(v)
	}
}

func setFloat(f *reflect.Value, value interface{}) {
	if v, ok := value.(float64); ok {
		if v != 0 {
			f.SetFloat(v)
		}
	} else if v, ok := value.(float32); ok {
		if v != 0 {
			f.SetFloat(float64(v))
		}
	} else {
		v := reflect.ValueOf(value).Convert(f.Type())
		f.Set(v)
	}
}

func setString(f *reflect.Value, value interface{}) {
	if str, ok := value.(string); ok {
		if str != "" {
			f.SetString(str)
		}
	} else {
		v := reflect.ValueOf(value).Convert(f.Type())
		f.Set(v)
	}
}

func setAny(f *reflect.Value, value interface{}) {
	v := reflect.ValueOf(value).Convert(f.Type())
	f.Set(v)
}
