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
	"sync"
	"sync/atomic"
	"time"

	"github.com/infodancer/implog/httplog"
	"github.com/infodancer/implog/logstore/mysql"

	"github.com/infodancer/implog/logstore"
)

var errorCount uint64
var totalCount uint64

func main() {
	var err error
	logtype := flag.String("logtype", "HTTP", "The log file type (valid: http, smtp; defaults to http)")
	dir := flag.String("logdir", "", "The directory containing log files to import, which will be recursively scanned")
	file := flag.String("logfile", "", "The log file to import")
	dbdriver := flag.String("dbdriver", "mysql", "The type of database to use as a log store (defaults to mysql)")
	dbconnection := flag.String("dbconnection", "", "The name or ip address of the database host")
	numCPU := flag.Int("cpu", 4, "The number of cpus to use simultaneously")
	droptables := flag.Bool("droptables", false, "Drop and recreate the table structure")
	logname := flag.String("name", "", "The name of the log being read (usually, the hostname of the virtual host)")
	flag.Parse()

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

	if *droptables {
		fmt.Printf("Removing existing tables...\n")
		err = store.Clear(context.Background())
		if err != nil {
			log.Println(err)
			return
		}
	}
	err = store.Init(context.Background())
	if err != nil {
		log.Println(err)
		return
	}
	defer store.Close()

	files := make([]string, 0)
	if len(*file) > 0 {
		files = append(files, *file)
	} else if len(*dir) > 0 {
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
	var wg sync.WaitGroup
	cpu := 0
	for _, lf := range files {
		if cpu >= *numCPU {
			wg.Wait()
			cpu = 0
		}
		cpu++
		wg.Add(1)
		go importLog(&wg, lf, *logname, *logtype, store)
	}
	wg.Wait()
	log.Printf("Total inserted %v; total errors %v\n", totalCount, errorCount)
}

// importLog imports a line oriented log file, transparently handling gzip compression
func importLog(wg *sync.WaitGroup, file string, logname string, logtype string, store logstore.LogStore) error {
	defer wg.Done()
	var fileInsertCount uint64
	var fileErrorCount uint64
	start := time.Now()

	// Get the last modified time of the logfile
	info, err := os.Stat(file)
	if err != nil {
		log.Printf("could not stat %v\n", file)
		return err
	}

	// Compare it with the store modification time, if any
	_, modified, err := store.LookupLogFile(file, info.ModTime())
	if err != nil {
		return err
	}

	// Check the date comparison and return if nothing new
	if modified.After(info.ModTime()) || modified.Equal(info.ModTime()) {
		return nil
	}

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
				continue
			}
			entrydata.SetLogName(logname)
			entrydata.SetLogFile(file)
			entrydata.SetLogFileModified(info.ModTime())
			err = store.WriteHTTPLogEntry(ctx, entrydata)
			if err != nil {
				if !strings.Contains(err.Error(), "Duplicate entry") {
					log.Printf("error adding to store: %v", err)
					fileErrorCount++
				}
			} else {
				fileInsertCount++
			}
		}
		lc++
	}
	err = scanner.Err()
	if err != nil {
		log.Printf("error: %v", err)
	}

	t := time.Now()
	elapsed := t.Sub(start)
	atomic.AddUint64(&errorCount, fileErrorCount)
	atomic.AddUint64(&totalCount, fileInsertCount)

	if fileInsertCount > 0 {
		log.Printf("Processing: %v\n", file)
		log.Printf("parsed %v lines in %v taking %v \n", lc, file, elapsed)
		log.Printf("inserted %v; errors %v\n", fileInsertCount, fileErrorCount)
	}
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
