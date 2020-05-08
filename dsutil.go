package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/rustyx/dsutil/dsio"
	"google.golang.org/api/iterator"
)

var (
	project     = flag.String("project", "", "Google Cloud project name")
	kind        = flag.String("kind", "", "DataStore table name")
	filter      = flag.String("filter", "", "Filter field name (optional)")
	from        = flag.String("from", "", "Filter >= value (optional)")
	to          = flag.String("to", "", "Filter < value (optional)")
	skipdefault = flag.Bool("skipdefault", false, "skip default values (in 'convert' command)")
	// httpPort    = flag.Int("pprof", 0, "pprof listen port (e.g. 8080)") // for debugging
)

func main() {
	flag.Parse()
	// if *httpPort > 0 {
	// 	go func() { log.Fatal(http.ListenAndServe(fmt.Sprintf("localhost:%d", *httpPort), nil)) }()
	// }
	if len(flag.Args()) == 0 {
		printUsageAndDie("Missing command argument\n")
	}
	cmd := flag.Args()[0]
	switch cmd {
	case "export":
		cmdExport()
		return
	case "import":
		cmdImport()
		return
	case "delete":
		cmdDelete()
		return
	case "convert":
		cmdConvert()
		return
	case "test":
		cmdTest()
	default:
		printUsageAndDie("Invalid command argument\n")
	}
}

func printUsageAndDie(msg string) {
	fmt.Println(msg + `Usage: dsutil [options] command <args>
  command:
    export <filename>    - export records from DataStore
    import <filename>... - import records into DataStore
    delete               - delete records from DataStore
    convert <in> <out>   - convert exported records from JSON to Go object notation
`)
	flag.PrintDefaults()
	os.Exit(1)
}

func ensureRequiresArguments() {
	cmd := ""
	if len(flag.Args()) > 0 {
		cmd = flag.Args()[0]
	}
	switch {
	case *project == "" && cmd != "convert":
		printUsageAndDie("Missing required option -project\n")
	case *kind == "" && (cmd == "export" || cmd == "delete"):
		printUsageAndDie("Missing required option -kind\n")
	case len(flag.Args()) < 2 && (cmd == "export" || cmd == "import"):
		printUsageAndDie("Missing required argument <filename>\n")
	case len(flag.Args()) > 2 && cmd == "export":
		printUsageAndDie("Too many arguments for export command\n")
	case *filter != "" && *from == "" && *to == "":
		printUsageAndDie("Missing option -from and/or -to for -filter\n")
	case *filter == "" && *from != "":
		printUsageAndDie("Missing option -filter for -from\n")
	case *filter == "" && *to != "":
		printUsageAndDie("Missing option -filter for -to\n")
	}
}

func cmdExport() {
	ensureRequiresArguments()
	outfile, err := os.OpenFile(flag.Args()[1], os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	check(err, flag.Args()[1])
	defer outfile.Close()
	ds := connectDS()
	defer ds.Close()
	q := datastore.NewQuery(*kind)
	if *from != "" {
		q = q.Filter(fmt.Sprintf("%s>=", *filter), *from)
	}
	if *to != "" {
		q = q.Filter(fmt.Sprintf("%s<", *filter), *to)
	}
	it := ds.Run(context.Background(), q)
	err = dsio.Export(it, outfile)
	check(err, "ds.Export")
}

func cmdImport() {
	ensureRequiresArguments()
	for _, ff := range flag.Args()[1:] {
		filenames, err := filepath.Glob(ff)
		check(err, "Glob")
		for _, f := range filenames {
			log.Printf("Importing file %s", f)
			importFile(f)
		}
	}
	log.Printf("Done")
}

func importFile(filename string) {
	infile, err := os.Open(filename)
	check(err, filename)
	defer infile.Close()
	ds := connectDS()
	defer ds.Close()
	err = dsio.Import(ds, infile)
	check(err, "ds.Import")
}

func check(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func cmdDelete() {
	ensureRequiresArguments()
	ds := connectDS()
	defer ds.Close()
	q := datastore.NewQuery(*kind)
	if *from != "" {
		q = q.Filter(fmt.Sprintf("%s>=", *filter), *from)
	}
	if *to != "" {
		q = q.Filter(fmt.Sprintf("%s<", *filter), *to)
	}
	q.KeysOnly()
	it := ds.Run(context.Background(), q)
	n := 0
	var keys []*datastore.Key
	for {
		key, err := it.Next(nil)
		if err == iterator.Done {
			break
		}
		check(err, "ds.Next")
		log.Printf("Deleting %v", key)
		keys = append(keys, key)
		n++
		if len(keys) >= 200 {
			err = ds.DeleteMulti(context.Background(), keys)
			check(err, "ds.DeleteMulti")
			keys = nil
		}
	}
	if len(keys) > 0 {
		err := ds.DeleteMulti(context.Background(), keys)
		check(err, "ds.DeleteMulti")
	}
	log.Printf("Deleted %v", n)
}

func cmdConvert() {
	ensureRequiresArguments()
	if len(flag.Args()) != 3 {
		printUsageAndDie("convert arguments should be <in> <out>\n")
	}
	in, err := os.Open(flag.Args()[1])
	check(err, flag.Args()[1])
	defer in.Close()
	out, err := os.OpenFile(flag.Args()[2], os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	check(err, flag.Args()[2])
	defer out.Close()
	rbuf := bufio.NewScanner(in)
	rbuf.Buffer(make([]byte, 32768), 1024*1024*1024)
	wbuf := bufio.NewWriterSize(out, 32768)
	defer wbuf.Flush()
	inCh := make(chan []byte, 10)
	outCh := make(chan dsio.Entity, 10)
	errCh := make(chan error, 1)
	werrCh := make(chan error, 1)
	go dsio.Unmarshal(inCh, outCh, errCh)
	go func() {
		defer close(werrCh)
		for rec := range outCh {
			_, _ = wbuf.WriteString("{")
			i := 0
			for _, p := range rec.Properties {
				if *skipdefault && (p.Value == "" || p.Value == int64(0) || p.Value == float64(0.0) || p.Value == false) {
					continue
				}
				if i > 0 {
					_, _ = wbuf.WriteString(",")
				}
				i++
				value := p.Value
				switch v := value.(type) {
				case string:
					value = strconv.Quote(v)
				case time.Time:
					value = v.Format(`"2006-01-02T15:04:05.000Z"`)
					// case []byte: // TODO
				}
				_, err = wbuf.WriteString(fmt.Sprintf("%s:%v", p.Name, value))
				if err != nil {
					werrCh <- err
				}
			}
			_, _ = wbuf.WriteString("},\n")
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
	check(err, "parse")
	check(<-errCh, "parse")
	check(<-werrCh, "write")
}

func cmdTest() {
	ensureRequiresArguments()
	ds := connectDS()
	defer ds.Close()
	key := datastore.IDKey("Test", 0, nil)
	dt, err := time.Parse("20060102-15:04:05.000", "20060102-15:04:05.012")
	check(err, "parse time")
	ent := struct {
		Str   string
		Bool  bool
		Int   int
		Int32 int32
		Flt32 float32
		Flt   float64
		Dt    time.Time
		Bin   []byte
	}{
		"Test str",
		true,
		123,
		int32(123),
		float32(123.12),
		123.12,
		dt,
		[]byte{1, 2, 3},
	}
	key, err = ds.Put(context.Background(), key, &ent)
	check(err, "ds put")
	log.Printf("put=%v,%v", key, &ent)
	err = ds.Get(context.Background(), key, &ent)
	check(err, "ds get")
	log.Printf("get=%v", &ent)
}

func connectDS() *datastore.Client {
	host := os.Getenv("DATASTORE_EMULATOR_HOST")
	fmt.Printf("DATASTORE_EMULATOR_HOST=%q\n", host)
	ds, err := datastore.NewClient(context.Background(), *project)
	check(err, "DataStore")
	return ds
}
