package dsio

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
)

func Import(ds *datastore.Client, r io.Reader) (err error) {
	rbuf := bufio.NewScanner(r)
	rbuf.Buffer(make([]byte, 32768), 1024*1024*1024)
	inCh := make(chan []byte, 10)
	outCh := make(chan Entity, 10)
	errCh := make(chan error, 1)
	werrCh := make(chan error, 1)
	go Unmarshal(inCh, outCh, errCh)
	go func() {
		defer close(werrCh)
		var keys []*datastore.Key
		var rows []datastore.PropertyList
		batchSize := 200
		for rec := range outCh {
			keys = append(keys, rec.Key)
			rows = append(rows, rec.Properties)
			if len(rows) >= batchSize {
				_, err := ds.PutMulti(context.Background(), keys, rows)
				if err != nil {
					werrCh <- err
					return
				}
				keys, rows = nil, nil
			}
		}
		if len(rows) > 0 {
			_, err := ds.PutMulti(context.Background(), keys, rows)
			if err != nil {
				werrCh <- err
			}
		}
	}()
outer:
	for rbuf.Scan() {
		select {
		case inCh <- append([]byte{}, rbuf.Bytes()...): // make a copy
		case err = <-errCh:
			break outer
		}
	}
	close(inCh)
	err2 := <-errCh  // catch possible error at last line
	err3 := <-werrCh // wait for completion
	if err == nil {
		err = err2
	}
	if err == nil {
		err = err3
	}
	return
}

func Unmarshal(inCh <-chan []byte, outCh chan<- Entity, errCh chan<- error) {
	defer close(errCh)
	defer close(outCh)
	fieldtypes := make(map[string]string)
	fields := make(map[int]jsonField)
	e := struct {
		jsonRowReader
		jsonFields
	}{}
	linenr := 0
	for b := range inCh {
		linenr++
		if len(e.Row) < len(fields) {
			e.Row = make([]valueWrapper, len(fields))
			for i, f := range fields {
				e.Row[i].typ = f.Type
			}
		}
		e.Fields = nil
		e.Key = ""
		for i := range e.Row {
			e.Row[i].value = nil
		}
		err := json.Unmarshal(b, &e)
		if err != nil {
			errCh <- fmt.Errorf("line %d JSON Unmarshal error: %v. Line: %v", linenr, err, string(b))
			return
		}
		if len(e.Fields) > 0 {
			for i, f := range e.Fields {
				f.idx = i + e.FieldsFrom
				fields[f.idx] = f
				fieldtypes[f.Name] = f.Type
			}
			e.Row = nil
		}
		if e.Key == "" || len(e.Row) == 0 {
			continue
		}
		rec := Entity{Key: UnmarshalKey(e.Key)}
		for i, v := range e.Row {
			if v.value == nil {
				continue
			}
			f := fields[i]
			p := datastore.Property{
				Name:    f.Name,
				Value:   v.value,
				NoIndex: f.NoIndex,
			}
			rec.Properties = append(rec.Properties, p)
		}
		outCh <- rec
	}
}

type jsonRowReader struct {
	Key string         `json:"k"`
	Row []valueWrapper `json:"d"`
}

type valueWrapper struct { // implements json.Unmarshaler
	typ   string
	value interface{}
}

func (v *valueWrapper) UnmarshalJSON(b []byte) (err error) {
	if len(b) == 4 && string(b) == "null" {
		v.value = nil
		return nil
	}
	switch v.typ {
	case "bool":
		v.value, err = strconv.ParseBool(string(b))
	case "int64":
		v.value, err = strconv.ParseInt(string(b), 10, 64)
	case "float64":
		v.value, err = strconv.ParseFloat(string(b), 64)
	case "string":
		v.value, err = strconv.Unquote(string(b))
	case "time.Time":
		if len(b) > 2 && b[0] == '"' {
			v.value, err = time.Parse("2006-01-02T15:04:05.000Z", string(b[1:len(b)-1]))
		}
	case "[]uint8":
		if len(b) > 2 && b[0] == '"' {
			v.value, err = base64.RawStdEncoding.DecodeString(string(b[1 : len(b)-1]))
		}
	default:
		err = fmt.Errorf("Unsupported data type '%s'", v.typ)
	}
	if err != nil {
		err = fmt.Errorf("Unable to unmarshal '%v' as %v", string(b), v.typ)
	}
	return
}
