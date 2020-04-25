## dsutil

### Building:

```
go get github.com/rustyx/dsutil
go build github.com/rustyx/dsutil
```

### Usage:

```
dsutil [options] command <args>

  command:
    export <filename>    - export records from DataStore
    import <filename>... - import records into DataStore
    delete               - delete records from DataStore

  -project string
    	Google Cloud project name
  -kind string
    	DataStore table name
  -filter string
    	Filter field name (optional)
  -from string
    	Filter >= value (optional)
  -to string
    	Filter < value (optional)
```

