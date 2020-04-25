package dsio

import (
	"testing"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUnmarshal(t *testing.T) {
	inCh := make(chan []byte, 10)
	outCh := make(chan Entity, 10)
	errCh := make(chan error, 2)
	go Unmarshal(inCh, outCh, errCh)
	inCh <- []byte(`{"FieldsFrom":0,"Fields":[
		{"n":"Str","t":"string","i":false},
		{"n":"Str2","t":"string","i":false},
		{"n":"Int","t":"int64","i":false},
		{"n":"Flt","t":"float64","i":false},
		{"n":"Dt","t":"time.Time","i":false},
		{"n":"Bool","t":"bool","i":false},
		{"n":"Bin","t":"[]uint8","i":false}
	]}`)
	inCh <- []byte(`{"k":"/Test,1","d":["Test str \"A\"",null,123,123.12,"2006-01-02T15:04:05.012Z",true,"AQID"]}`)
	close(inCh)
	require.NoError(t, <-errCh)
	res := <-outCh
	assert.EqualValues(t, datastore.IDKey("Test", 1, nil), res.Key)
	dt, err := time.Parse("2006-01-02T15:04:05.000Z", "2006-01-02T15:04:05.012Z")
	require.NoError(t, err)
	assert.EqualValues(t, datastore.PropertyList{
		{Name: "Str", Value: `Test str "A"`},
		{Name: "Int", Value: int64(123)},
		{Name: "Flt", Value: float64(123.12)},
		{Name: "Dt", Value: dt},
		{Name: "Bool", Value: true},
		{Name: "Bin", Value: []byte{1, 2, 3}},
	}, res.Properties)
}
