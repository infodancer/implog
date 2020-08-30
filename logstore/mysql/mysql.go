package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	// Load the mysql driver
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/infodancer/implog/httplog"
)

// LogStore implements a log store in mysql
type LogStore struct {
	dbdriver        string
	dbconnection    string
	lfcMutex        *sync.Mutex
	ipcMutex        *sync.Mutex
	uriMutex        *sync.Mutex
	referMutex      *sync.Mutex
	logfilecache    map[string]string
	ipcache         map[string]string
	uricache        map[string]string
	refercache      map[string]string
	insertLogEntry  *sql.Stmt
	insertLogFile   *sql.Stmt
	selectLogFile   *sql.Stmt
	updateLogFile   *sql.Stmt
	insertIPAddress *sql.Stmt
	selectIPAddress *sql.Stmt
	insertURI       *sql.Stmt
	selectURI       *sql.Stmt
	insertReferrer  *sql.Stmt
	selectReferrer  *sql.Stmt
	db              *sql.DB
}

const createTable = "CREATE TABLE IF NOT EXISTS "
const dropTable = "DROP TABLE IF EXISTS "
const idField = "id BINARY(16) PRIMARY KEY"
const createLogFileTable = createTable + "LOGFILE (" + idField + ", filename VARCHAR(255), modified TIMESTAMP, created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
const createLogURITable = createTable + "LOGURI (" + idField + ", uri VARCHAR(255), created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
const createLogIPTable = createTable + "LOGIP (" + idField + ", ip VARCHAR(16), name VARCHAR(255), created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
const createLogReferrerTable = createTable + "LOGREFERRER (" + idField + ", uri VARCHAR(255), created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
const createLogEntryTable = createTable + "LOGENTRY (" + idField + ", logfile_id INT, loguri_id INT, ipaddress varchar(16), clientident varchar(255), clientauth varchar(255), clientversion varchar(255), requestmethod VARCHAR(16), requestprotocol VARCHAR(16), size BIGINT, status INT, referrer VARCHAR(255))"
const createClientTable = createTable + "CLIENT ()"
const dropLogFileTable = dropTable + " LOGFILE"
const dropLogEntryTable = dropTable + " LOGENTRY"
const dropLogURITable = dropTable + " LOGURI"
const dropLogReferrerTable = dropTable + " LOGREFERRER"
const dropLogIPTable = dropTable + " LOGIP"
const insertQuery = "INSERT INTO LOGENTRY(id, logfile_id, loguri_id, ipaddress, clientident, clientauth, clientversion, requestmethod, requestprotocol, size, status, referrer) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

// New defines the connection information for the log store
func New(dbdriver string, dbconnection string) (*LogStore, error) {
	result := LogStore{}
	result.dbconnection = dbconnection
	result.dbdriver = dbdriver
	result.ipcMutex = &sync.Mutex{}
	result.lfcMutex = &sync.Mutex{}
	result.uriMutex = &sync.Mutex{}
	result.referMutex = &sync.Mutex{}
	return &result, nil
}

// Clear drops the tables used for storing log data, normally so they can be recreated in a new format
func (s *LogStore) Clear(ctx context.Context) error {
	var err error
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	s.db.Begin()
	defer tx.Rollback()

	_, err = s.db.Exec(dropLogEntryTable)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(dropLogFileTable)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(dropLogURITable)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(dropLogReferrerTable)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(dropLogIPTable)
	if err != nil {
		return err
	}
	return nil
}

// Open creates a connection to the log store
func (s *LogStore) Open() error {
	var err error
	// log.Printf("dbconnection: %v\n", s.dbconnection)
	s.db, err = sql.Open(s.dbdriver, s.dbconnection)
	if err != nil {
		log.Fatal(err)
		return err
	}

	return nil
}

// Ping creates the table structure for storing records, if necessary
func (s *LogStore) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// Init creates the table structure for storing records, if necessary
func (s *LogStore) Init(ctx context.Context) error {
	fmt.Printf("Init()\n")
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
		return err
	}
	s.db.Begin()
	defer tx.Rollback()
	s.db.SetConnMaxLifetime(0)

	fmt.Printf("Init: %v\n", createLogFileTable)
	_, err = s.db.Exec(createLogFileTable)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Init: %v\n", createLogURITable)
	_, err = s.db.Exec(createLogURITable)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Init: %v\n", createLogIPTable)
	_, err = s.db.Exec(createLogIPTable)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Init: %v\n", createLogReferrerTable)
	_, err = s.db.Exec(createLogReferrerTable)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Printf("Init: %v\n", createLogEntryTable)
	_, err = s.db.Exec(createLogEntryTable)
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.logfilecache = make(map[string]string)
	s.ipcache = make(map[string]string)
	s.uricache = make(map[string]string)
	s.refercache = make(map[string]string)

	s.selectLogFile, err = s.db.PrepareContext(ctx, "SELECT id,modified FROM LOGFILE WHERE filename = ?")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.insertLogFile, err = s.db.PrepareContext(ctx, "INSERT INTO LOGFILE (id, filename, created) VALUES (?,?,?)")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.updateLogFile, err = s.db.PrepareContext(ctx, "UPDATE LOGFILE SET modified = ? where id = ?")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.selectIPAddress, err = s.db.PrepareContext(ctx, "SELECT id FROM LOGIP WHERE ip = ?")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.insertIPAddress, err = s.db.PrepareContext(ctx, "INSERT INTO LOGIP (id, ip, name) VALUES (?,?,?)")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.selectURI, err = s.db.PrepareContext(ctx, "SELECT id FROM LOGURI WHERE uri = ?")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.insertURI, err = s.db.PrepareContext(ctx, "INSERT INTO LOGURI (id, uri) VALUES (?,?)")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.selectReferrer, err = s.db.PrepareContext(ctx, "SELECT id FROM LOGREFERRER WHERE uri = ?")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.insertReferrer, err = s.db.PrepareContext(ctx, "INSERT INTO LOGREFERRER (id, uri) VALUES (?,?)")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.insertLogEntry, err = s.db.PrepareContext(ctx, insertQuery)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

// Close closes the database connection
func (s *LogStore) Close() {
	s.insertLogEntry.Close()
	s.selectLogFile.Close()
	s.insertLogFile.Close()
	s.selectURI.Close()
	s.insertURI.Close()
	s.selectIPAddress.Close()
	s.insertIPAddress.Close()
	s.selectReferrer.Close()
	s.insertReferrer.Close()
	return
}

// LookupURI retrieves the file id of a log file
func (s *LogStore) LookupURI(uri string) (string, error) {
	s.uriMutex.Lock()
	r := s.uricache[uri]
	s.uriMutex.Unlock()
	if r != "" {
		return r, nil
	}
	var id string
	err := s.selectURI.QueryRow(uri).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			id = uuid.New().String()
			_, err = s.insertURI.Exec(id, uri)
			if err != nil {
				log.Printf("insert err: %v", err)
				return "", err
			}
			s.uriMutex.Lock()
			s.uricache[uri] = id
			s.uriMutex.Unlock()
			return id, nil
		}
		log.Printf("select err: %v", err)
		return "", err
	}
	return id, nil
}

// LookupLogFile retrieves the file id of a log file
func (s *LogStore) LookupLogFile(logfile string, modified time.Time) (string, time.Time, error) {
	s.lfcMutex.Lock()
	r := s.logfilecache[logfile]
	s.lfcMutex.Unlock()
	if r != "" {
		return r, modified, nil
	}
	var row struct {
		id       string
		modified time.Time
	}
	var nt mysql.NullTime
	err := s.selectLogFile.QueryRow(logfile).Scan(&row.id, &nt)
	if err != nil {
		if err == sql.ErrNoRows {
			// insert a new record
			row.id = uuid.New().String()
			_, err = s.insertLogFile.Exec(row.id, logfile, modified)
			if err != nil {
				log.Printf("insert err: %v", err)
				return "", modified, err
			}
			s.lfcMutex.Lock()
			s.logfilecache[logfile] = row.id
			s.lfcMutex.Unlock()

			// return yesterday's date to ensure the new filw is processed
			yesterday := time.Now().AddDate(0, 0, -1)
			return row.id, yesterday, nil
		}
		log.Printf("select err: %v", err)
		return "", modified, err
	}
	// Handle nulltime
	if nt.Valid {
		row.modified = nt.Time
	} else {
		row.modified = time.Now().AddDate(0, 0, -1)
	}
	// Compare the modified time and update if needed
	if modified.After(row.modified) {
		_, err = s.updateLogFile.Exec(modified, logfile)
		if err != nil {
			log.Printf("update err: %v", err)
			return row.id, row.modified, err
		}
		s.lfcMutex.Lock()
		s.logfilecache[logfile] = row.id
		s.lfcMutex.Unlock()
	}

	return row.id, row.modified, nil
}

// LookupIPAddress retrieves the uuid for an ip address
func (s *LogStore) LookupIPAddress(ip string) (string, error) {
	s.ipcMutex.Lock()
	r := s.ipcache[ip]
	s.ipcMutex.Unlock()
	if r != "" {
		return r, nil
	}
	var id string
	err := s.selectIPAddress.QueryRow(ip).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			id = uuid.New().String()
			var name string
			names, err := net.LookupAddr(ip)
			if err != nil || len(names) < 1 {
				name = "unknown"
			} else {
				name = names[0]
			}
			_, err = s.insertIPAddress.Exec(id, ip, name)
			if err != nil {
				log.Printf("insert err: %v", err)
				return "", err
			}
			s.ipcMutex.Lock()
			s.ipcache[ip] = id
			s.ipcMutex.Unlock()
			return id, nil
		}
		log.Printf("select err: %v", err)
		return "", err
	}
	return id, nil
}

// LookupReferrer retrieves the referrer
func (s *LogStore) LookupReferrer(referrer string) (string, error) {
	s.referMutex.Lock()
	r := s.refercache[referrer]
	s.referMutex.Unlock()
	if r != "" {
		return r, nil
	}
	var id string
	err := s.selectReferrer.QueryRow(referrer).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			id = uuid.New().String()
			_, err = s.insertReferrer.Exec(id, referrer)
			if err != nil {
				log.Printf("insert err: %v", err)
				return "", err
			}
			s.referMutex.Lock()
			s.refercache[referrer] = id
			s.referMutex.Unlock()
			return id, nil
		}
		log.Printf("select err: %v", err)
		return "", err
	}
	return id, nil
}

// WriteHTTPLogEntry writes an http log entry to the log store
func (s *LogStore) WriteHTTPLogEntry(ctx context.Context, entry httplog.Entry) error {
	if entry.IsParseError() {
		return nil
	}
	uuid := base64.URLEncoding.EncodeToString(entry.GetUUID())
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		log.Println(err)
		return err
	}
	defer tx.Rollback()
	// log.Printf("UUID: %v", uuid)

	// Look up logfile (inserting if necessary)
	fileID, _, err := s.LookupLogFile(entry.GetLogFile(), entry.GetLogFileModified())

	// Look up ip address (inserting if necessary)
	s.LookupIPAddress(entry.GetIPAddress())
	// Look up URI (inserting if necessary)
	uriID, err := s.LookupURI(entry.GetRequestURI())
	// Look up referrer (inserting if necessary)
	referrerID, err := s.LookupReferrer(entry.GetReferrer())
	// Insert log itself

	_, err = s.insertLogEntry.ExecContext(ctx, uuid, fileID, uriID, entry.GetIPAddress(), entry.GetClientIdent(),
		entry.GetClientAuth(), entry.GetClientVersion(), entry.GetRequestMethod(), entry.GetRequestProtocol(),
		entry.GetSize(), entry.GetStatus(), referrerID)
	if err != nil {
		log.Printf("error inserting %v: %v", uuid, err)
		return err
	}
	tx.Commit()

	return nil
}
