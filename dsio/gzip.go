package dsio

import (
	"compress/gzip"
	"io"
	"os"
	"strings"
)

type gzipReader struct {
	io.ReadCloser
	file io.ReadCloser
}

// OpenForReading opens a file for reading, seamlessly un-gzipping if needed.
func OpenForReading(filename string) (io.ReadCloser, error) {
	infile, err := os.Open(filename)
	if err != nil {
		return infile, err
	}
	if strings.HasSuffix(filename, "gz") {
		gz, err := gzip.NewReader(infile)
		if err != nil {
			infile.Close()
			return nil, err
		}
		return &gzipReader{gz, infile}, nil
	}
	return infile, err
}

func (g *gzipReader) Close() error {
	errgzip := g.ReadCloser.Close()
	errfile := g.file.Close()
	if errgzip != nil {
		return errgzip
	}
	return errfile
}

type gzipWriter struct {
	io.WriteCloser
	file io.WriteCloser
}

// OpenForWriting opens a file for writing, seamlessly gzipping if needed.
func OpenForWriting(filename string) (io.WriteCloser, error) {
	infile, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return infile, err
	}
	if strings.HasSuffix(filename, "gz") {
		gz := gzip.NewWriter(infile)
		return &gzipWriter{gz, infile}, nil
	}
	return infile, err
}

func (g *gzipWriter) Close() error {
	errgzip := g.WriteCloser.Close()
	errfile := g.file.Close()
	if errgzip != nil {
		return errgzip
	}
	return errfile
}
