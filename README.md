## dsutil - Google Cloud DataStore import/export utilities

[![Build Status](https://app.travis-ci.com/rustyx/dsutil.svg?token=yyneiMmW2WmpN8XP4VXe&branch=master)](https://app.travis-ci.com/rustyx/dsutil)

### Installing

```
go get github.com/rustyx/dsutil
```

### Command Line Usage

```
dsutil [options] command <args>

  command:
    export <filename>          - export records from DataStore
    import <filename>...       - import records into DataStore
    delete                     - delete records from DataStore
    set <field> <type> <value> - update records in DataStore (type is: string, int, double)
    convert <in> <out>         - convert exported records from JSON to Go object notation
  Note: <filename> ending with ".gz" will be automatically g(un)zipped

  -project string
    	Google Cloud project name (deduced if not provided)
  -kind string
    	DataStore table name (required for export)
  -filter string
    	Filter field name (optional)
  -from string
    	Filter >= value (optional)
  -to string
    	Filter < value (optional)
  -eq string
    	Filter = value (optional)
```

### API Usage

It is possible to read an export file and process each entity programmatically.

There are two interfaces: [`ImportFile`](https://pkg.go.dev/github.com/rustyx/dsutil/dsio#ImportFile), based on key-value pairs, and [`ImportFileReflect`](https://pkg.go.dev/github.com/rustyx/dsutil/dsio#ImportFileReflect), which is useful for ORM. Here's an example of how to load an export file into a PostgreSQL database using `go-pg` ORM API:

```
type MyEntity struct {
	Id          int    `datastore:"-"`
	SomeColumn  string `pg:"type:varchar(40)"`
	SomeColumn2 string `pg:"type:varchar(40)"`
	// . . .
}

	inputFile := "my-export.ds"
	log.Printf("Importing %v", inputFile)
	insertFunc := func(kind string, rows []interface{}) error {
		log.Printf("Inserting %v %s(s)", len(rows), kind)
		_, err := pgdb.Model(rows...).Insert()
		return err
	}
	modelMap := []dsio.ModelMapping{
		{Kind: "MyEntity", TypePtr: &MyEntity{}, ImportFunc: insertFunc, BatchSize: 200},
	}
	if err := dsio.ImportFileReflect(inputFile, modelMap); err != nil {
		log.Fatalf("import %v failed: %v", inputFile, err)
	}
```

For more documentation refer to [API Docs](https://pkg.go.dev/github.com/rustyx/dsutil/dsio).
