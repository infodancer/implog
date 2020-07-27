package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/infodancer/implog/httplog"
	"github.com/infodancer/implog/logstore/mysql"

	"github.com/infodancer/implog/logstore"
)

func main() {
	var err error
	logtype := flag.String("logtype", "HTTP", "The log file type (valid: http, smtp; defaults to http)")
	dir := flag.String("logdir", "", "The directory containing log files to import, which will be recursively scanned")
	file := flag.String("logfile", "", "The log file to import")
	dbdriver := flag.String("dbdriver", "mysql", "The type of database to use as a log store (defaults to mysql)")
	dbconnection := flag.String("dbconnection", "", "The name or ip address of the database host")
	logname := flag.String("logname", "", "The name of the log being read (usually, the hostname of the virtual host)")
	flag.Parse()

	fmt.Printf("Opening logstore...\n")
	var store logstore.LogStore
	if *dbdriver == "mysql" {
		store, err = mysql.New(*dbdriver, *dbconnection)
		if err != nil {
			log.Println(err)
			return
		}
		err = store.Open()
		if err != nil {
			log.Println(err)
			return
		}
		err = store.Ping(context.Background())
		if err != nil {
			log.Println(err)
			return
		}
	} else {
		fmt.Printf("Unrecognized logstore type!")
	}

	fmt.Printf("Initializing logstore...\n")
	fmt.Printf("Removing existing tables...\n")
	err = store.Clear(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Printf("Creating new table structure...\n")
	err = store.Init(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	fmt.Printf("Initialization complete\n")
	defer store.Close()

	files := make([]string, 0)
	if len(*file) > 0 {
		files = append(files, *file)
	} else if len(*dir) > 0 {
		fmt.Printf("reading dir: %v\n", *dir)
		filecheck := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				log.Println(err)
				return nil
			}
			if strings.Contains(path, "access_log") {
				// log.Println(path, info.Size())
				files = append(files, path)
			}
			return nil
		}

		err := filepath.Walk(*dir, filecheck)
		if err != nil {
			log.Println(err)
			return
		}
	}

	log.Printf("Entering log processing loop for log %v...\n", *logname)
	for _, lf := range files {
		importLog(lf, *logtype, store)
	}
}

// importLog imports a line oriented log file, transparently handling gzip compression
func importLog(file string, logtype string, store logstore.LogStore) error {
	log.Printf("Processing: %v\n", file)

	f, err := os.Open(file)
	if err != nil {
		log.Printf("could not read %v\n", file)
		return err
	}
	defer f.Close()

	bReader := bufio.NewReader(f)
	var scanner *bufio.Scanner

	// If we detect gzip, then make a gzip reader, then wrap it in a scanner
	// log.Printf("Checking for compression...\n")
	gzipped, err := isFileContentGzip(bReader)
	if err != nil {
		log.Printf("err checking compression: %v\n", err)
		return err
	}
	if gzipped {
		log.Println("Detected gzipped input, decompressing...")
		gzipReader, err := gzip.NewReader(bReader)
		if err != nil {
			log.Printf("err during decompression: %v\n", err)
			return err
		}
		scanner = bufio.NewScanner(gzipReader)
	} else {
		scanner = bufio.NewScanner(bReader)
	}

	var lc int64
	for scanner.Scan() {
		ctx := context.Background()
		line := scanner.Text()
		if strings.EqualFold(logtype, "HTTP") {
			entrydata, err := httplog.ParseLogLine(line)
			if err != nil {
				log.Printf("error parsing line %v in %v: %v\n", lc, file, err)
				log.Println(line)
			}
			err = store.WriteHTTPLogEntry(ctx, entrydata)
			if err != nil {
				log.Printf("error: %v", err)
			}
		}
		lc++
	}
	err = scanner.Err()
	if err != nil {
		log.Printf("error: %v", err)
	}
	log.Printf("parsed %v lines in %v\n", lc, file)
	return nil
}

func isFileContentGzip(bReader *bufio.Reader) (bool, error) {
	testBytes, err := bReader.Peek(2)
	if err != nil {
		return false, err
	}
	if testBytes[0] == 31 && testBytes[1] == 139 {
		return true, nil
	}
	return false, nil
}
