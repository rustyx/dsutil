package dsio

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"time"

	"cloud.google.com/go/datastore"
	"google.golang.org/api/iterator"
)

// Entity wraps a DataStore entity including its key and all properties as key-value pairs.
type Entity struct {
	Key        *datastore.Key
	Properties datastore.PropertyList
}

// Export exports the given DataStore entity iterator into the given stream.
func Export(it *datastore.Iterator, w io.Writer) (err error) {
	wbuf := bufio.NewWriterSize(w, 32768)
	defer wbuf.Flush()
	inCh := make(chan Entity, 10)
	outCh := make(chan []byte, 10)
	errCh := make(chan error, 1)
	werrCh := make(chan error, 1)
	go Marshal(inCh, outCh, errCh)
	go func() {
		for b := range outCh {
			if _, err := wbuf.Write(b); err != nil {
				werrCh <- err
				break
			}
			if err := wbuf.WriteByte('\n'); err != nil {
				werrCh <- err
				break
			}
		}
		close(werrCh)
	}()
outer:
	for {
		rec := Entity{}
		rec.Key, err = it.Next(&rec.Properties)
		if err != nil {
			if err == iterator.Done {
				err = nil
			}
			break
		}
		select {
		case inCh <- rec:
		case err = <-errCh:
			break outer
		}
	}
	close(inCh)
	go func() {
		for range outCh {
			// drain outCh
		}
	}()
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

// Marshal marshals a stream of DataStore entities into a stream of byte arrays.
func Marshal(inCh <-chan Entity, outCh chan<- []byte, errCh chan<- error) {
	defer close(errCh)
	defer close(outCh)
	fields := make(map[string]jsonField)
	for rec := range inCh {
		sort.Slice(rec.Properties, func(a, b int) bool {
			return rec.Properties[a].Name < rec.Properties[b].Name
		})
		newfields := jsonFields{}
		row := jsonRow{Key: MarshalKey(rec.Key)}
		for _, p := range rec.Properties {
			f, ok := fields[p.Name]
			if !ok {
				fields[p.Name] = jsonField{Name: p.Name, Type: fmt.Sprint(reflect.TypeOf(p.Value)), NoIndex: p.NoIndex, idx: len(fields)}
				f = fields[p.Name]
				if newfields.Fields == nil {
					newfields.FieldsFrom = f.idx
				}
				newfields.Fields = append(newfields.Fields, f)
			}
			value := prepareForMarshal(p.Value)
			if f.idx == len(row.Row) {
				row.Row = append(row.Row, value)
			} else {
				if f.idx > len(row.Row) {
					row.Row = append(row.Row, make([]interface{}, f.idx-len(row.Row)+1)...)
				}
				row.Row[f.idx] = value
			}
		}
		if len(newfields.Fields) != 0 {
			b, err := json.Marshal(newfields)
			if err != nil {
				errCh <- err
				return
			}
			outCh <- b
		}
		b, err := json.Marshal(row)
		if err != nil {
			errCh <- err
			return
		}
		outCh <- b
	}
}

func prepareForMarshal(value interface{}) interface{} {
	tm, ok := value.(time.Time)
	if ok {
		return tm.UTC().Format("2006-01-02T15:04:05.000Z") // DataStore does not store timezone
	}
	return value
}

type jsonField struct {
	Name    string `json:"n"`
	Type    string `json:"t"`
	NoIndex bool   `json:"i"`
	idx     int
}

type jsonFields struct {
	FieldsFrom int         `json:"FieldsFrom"`
	Fields     []jsonField `json:"Fields"`
}

type jsonRow struct {
	Key string        `json:"k"`
	Row []interface{} `json:"d"`
}
