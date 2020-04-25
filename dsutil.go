package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/rustyx/dsutil/dsio"
	"google.golang.org/api/iterator"
)

var (
	project = flag.String("project", "", "Google Cloud project name")
	kind    = flag.String("kind", "", "DataStore table name")
	filter  = flag.String("filter", "", "Filter field name (optional)")
	from    = flag.String("from", "", "Filter >= value (optional)")
	to      = flag.String("to", "", "Filter < value (optional)")
)

func main() {
	flag.Parse()
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
	case "test":
		doTest()
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
`)
	flag.PrintDefaults()
	os.Exit(1)
}

func ensureRequiresArguments() {
	switch {
	case *project == "":
		printUsageAndDie("Missing required option -project\n")
	case *kind == "":
		printUsageAndDie("Missing required option -kind\n")
	case len(flag.Args()) < 2 && (flag.Args()[0] == "export" || flag.Args()[0] == "import"):
		printUsageAndDie("Missing required argument <filename>\n")
	case len(flag.Args()) > 2 && flag.Args()[0] == "export":
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
	ds, err := datastore.NewClient(context.Background(), *project)
	check(err, "DataStore")
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
	ds, err := datastore.NewClient(context.Background(), *project)
	check(err, "DataStore")
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
	ds, err := datastore.NewClient(context.Background(), *project)
	check(err, "DataStore")
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
		err = ds.DeleteMulti(context.Background(), keys)
		check(err, "ds.DeleteMulti")
	}
	log.Printf("Deleted %v", n)
}

func doTest() {
	ensureRequiresArguments()
	ds, err := datastore.NewClient(context.Background(), *project)
	check(err, "DataStore")
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
