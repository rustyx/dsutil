package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	_ "net/http/pprof"
	"os"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/rustyx/dsutil/dsio"
	"google.golang.org/api/iterator"
)

var (
	project     = flag.String("project", "", "Google Cloud project name (deduced if not provided)")
	kind        = flag.String("kind", "", "DataStore table name (required for 'export')")
	filter      = flag.String("filter", "", "Filter field name (optional)")
	from        = flag.String("from", "", "Filter >= value (optional)")
	to          = flag.String("to", "", "Filter < value (optional)")
	eq          = flag.String("eq", "", "Filter = value (optional)")
	order       = flag.String("order", "", "Order by field name, use '-' prefix for descending order (optional)")
	limit       = flag.Int("limit", 0, "Max number of records to export (optional)")
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
	case "set":
		cmdSet()
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
    export <filename>          - export records from DataStore
    import <filename>...       - import records into DataStore
    delete                     - delete records from DataStore
    set <field> <type> <value> - update records in DataStore (type is: string, int, double)
    convert <in> <out>         - convert exported records from JSON to Go object notation
  Note: <filename> ending with ".gz" will be automatically g(un)zipped
`)
	flag.PrintDefaults()
	os.Exit(1)
}

func ensureRequiredArguments() {
	cmd := ""
	if len(flag.Args()) > 0 {
		cmd = flag.Args()[0]
	}
	if *project == "" {
		*project = deduceProjectID()
	}
	switch {
	case *project == "" && cmd != "convert":
		printUsageAndDie("Missing required option -project\n")
	case *kind == "" && cmd == "export":
		printUsageAndDie("Missing required option -kind\n")
	case len(flag.Args()) < 2 && (cmd == "export" || cmd == "import"):
		printUsageAndDie("Missing required argument <filename>\n")
	case len(flag.Args()) > 2 && cmd == "export":
		printUsageAndDie("Too many arguments for export command\n")
	case *filter != "" && *from == "" && *to == "" && *eq == "":
		printUsageAndDie("Missing option -from, -to or -eq for -filter\n")
	case *filter == "" && *from != "":
		printUsageAndDie("Missing option -filter for -from\n")
	case *filter == "" && *to != "":
		printUsageAndDie("Missing option -filter for -to\n")
	}
}

func cmdExport() {
	ensureRequiredArguments()
	outfile, err := dsio.OpenForWriting(flag.Args()[1])
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
	if *eq != "" {
		q = q.Filter(fmt.Sprintf("%s=", *filter), *eq)
	}
	if *order != "" {
		if *order == "1" {
			*order = *filter
		} else if *order == "-1" {
			*order = "-" + *filter
		}
		q = q.Order(*order)
	}
	if *limit != 0 {
		q = q.Limit(*limit)
	}
	it := ds.Run(context.Background(), q)
	err = dsio.Export(it, outfile)
	check(err, "ds.Export")
}

func cmdImport() {
	ensureRequiredArguments()
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
	infile, err := dsio.OpenForReading(filename)
	check(err, filename)
	defer infile.Close()
	ds := connectDS()
	defer ds.Close()
	err = dsio.Import(infile, ds)
	check(err, "ds.Import")
}

func check(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %v", msg, err)
	}
}

func cmdSet() {
	ensureRequiredArguments()
	ds := connectDS()
	defer ds.Close()
	if len(flag.Args()) != 4 {
		printUsageAndDie("'set' requires 3 arguments: FieldName, type and Value\n")
	}
	if *kind == "" {
		printUsageAndDie("Missing required option -kind\n")
	}
	var err error
	key, valueType, valueStr := flag.Args()[1], flag.Args()[2], flag.Args()[3]
	var value interface{}
	switch valueType {
	case "string":
		value = valueStr
	case "int":
		value, err = strconv.Atoi(valueStr)
		check(err, "Atoi")
	case "double":
		value, err = strconv.ParseFloat(valueStr, 64)
		check(err, "ParseFloat")
	default:
		check(errors.New("Invalid type "+valueType), "parse type")
	}
	log.Printf("Updating %s, setting %s=%v", *kind, key, value)
	q := datastore.NewQuery(*kind)
	if *from != "" {
		log.Printf("where %s >= %v", *filter, *from)
		q = q.Filter(fmt.Sprintf("%s>=", *filter), *from)
	}
	if *to != "" {
		log.Printf("where %s < %v", *filter, *to)
		q = q.Filter(fmt.Sprintf("%s<", *filter), *to)
	}
	if *eq != "" {
		log.Printf("where %s = %v", *filter, *eq)
		q = q.Filter(fmt.Sprintf("%s=", *filter), *eq)
	}
	it := ds.Run(context.Background(), q)
	n := 0
	for {
		rec := dsio.Entity{}
		rec.Key, err = it.Next(&rec.Properties)
		if err == iterator.Done {
			break
		}
		check(err, "ds.Next")
		found := false
		for i, p := range rec.Properties {
			if p.Name == key {
				rec.Properties[i].Value = value
				found = true
				break
			}
		}
		if !found {
			rec.Properties = append(rec.Properties, datastore.Property{Name: key, Value: value})
		}
		// log.Printf("Updating %v", rec.Key)
		_, err = ds.Put(context.Background(), rec.Key, &rec.Properties)
		check(err, "ds.Put")
		n++
	}
	log.Printf("Updated %v", n)
}

func cmdDelete() {
	ensureRequiredArguments()
	ds := connectDS()
	defer ds.Close()
	if *filter != "" && len(flag.Args()) > 1 {
		printUsageAndDie("'delete' supports -filter OR input file(s), not both\n")
	}
	if len(flag.Args()) > 1 {
		for _, ff := range flag.Args()[1:] {
			filenames, err := filepath.Glob(ff)
			check(err, "Glob")
			for _, f := range filenames {
				deleteFromFile(f, ds)
			}
		}
		return
	}
	if *kind == "" {
		printUsageAndDie("Missing required option -kind\n")
	}
	log.Printf("Deleting entities from %s", *kind)
	q := datastore.NewQuery(*kind)
	if *from != "" {
		log.Printf("where %s >= %v", *filter, *from)
		q = q.Filter(fmt.Sprintf("%s>=", *filter), *from)
	}
	if *to != "" {
		log.Printf("where %s < %v", *filter, *to)
		q = q.Filter(fmt.Sprintf("%s<", *filter), *to)
	}
	if *eq != "" {
		log.Printf("where %s = %v", *filter, *eq)
		q = q.Filter(fmt.Sprintf("%s=", *filter), *eq)
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

func deleteFromFile(filename string, ds *datastore.Client) {
	infile, err := dsio.OpenForReading(filename)
	check(err, filename)
	defer infile.Close()
	log.Printf("Deleting entities from file %s", filename)
	outCh := make(chan dsio.Entity, 10)
	errCh := make(chan error, 1)
	go func() {
		defer close(errCh)
		var keys []*datastore.Key
		batchSize := 200
		for rec := range outCh {
			log.Printf("Deleting %v", rec.Key)
			keys = append(keys, rec.Key)
			if len(keys) >= batchSize {
				err = ds.DeleteMulti(context.Background(), keys)
				if err != nil {
					errCh <- err
					return
				}
				keys = nil
			}
		}
		if len(keys) > 0 {
			err = ds.DeleteMulti(context.Background(), keys)
			if err != nil {
				errCh <- err
			}
		}
	}()
	err = dsio.ImportFile(infile, outCh, errCh)
	check(err, "ds.Delete")
}

func cmdConvert() {
	ensureRequiredArguments()
	if len(flag.Args()) != 3 {
		printUsageAndDie("convert arguments should be <in> <out>\n")
	}
	in, err := dsio.OpenForReading(flag.Args()[1])
	check(err, flag.Args()[1])
	defer in.Close()
	out, err := dsio.OpenForWriting(flag.Args()[2])
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
	ensureRequiredArguments()
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
	if host != "" {
		log.Printf("DATASTORE_EMULATOR_HOST=%q", host)
	}
	ds, err := datastore.NewClient(context.Background(), *project)
	check(err, "DataStore")
	return ds
}

func deduceProjectID() (res string) {
	if os.Getenv("DATASTORE_EMULATOR_HOST") != "" {
		res = os.Getenv("DATASTORE_PROJECT_ID")
	}
	if res == "" {
		active := strings.Trim(getConfigFile("gcloud/active_config"), " \t\r\n")
		if active == "" {
			active = "default"
		}
		cfg := getConfigFile("gcloud/configurations/config_" + active)
		re := regexp.MustCompile(`(?s)\[core\].*?\bproject = (\S+)`)
		m := re.FindStringSubmatch(cfg)
		if m != nil {
			res = m[1]
		}
	}
	if res != "" {
		log.Printf("Using DataStore project %v", res)
	}
	return
}

func getConfigFile(s string) string {
	var b []byte
	if runtime.GOOS == "windows" {
		b, _ = ioutil.ReadFile(filepath.Join(os.Getenv("APPDATA"), s))
	} else {
		b, _ = ioutil.ReadFile(filepath.Join(guessUnixHomeDir(), ".config", s))
	}
	return string(b)
}

func guessUnixHomeDir() string {
	if v := os.Getenv("HOME"); v != "" {
		return v
	}
	if u, err := user.Current(); err == nil {
		return u.HomeDir
	}
	return ""
}
