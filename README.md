## dsutil - Google Cloud DataStore import/export utilities

[![Build Status](https://travis-ci.com/rustyx/dsutil.svg?branch=master)](https://travis-ci.com/rustyx/dsutil)

### Installing:

```
go get github.com/rustyx/dsutil
```

### Usage:

```
dsutil [options] command <args>

  command:
    export <filename>          - export records from DataStore
    import <filename>...       - import records into DataStore
    delete                     - delete records from DataStore
    set <field> <type> <value> - update records in DataStore (type is: string, int, double)
    convert <in> <out>         - convert exported records from JSON to Go object notation

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
