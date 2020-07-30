package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"time"

	// Load the mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/infodancer/implog/httplog"
)

// LogStore implements a log store in mysql
type LogStore struct {
	dbdriver       string
	dbconnection   string
	logfilecache   map[string]string
	insertLogEntry *sql.Stmt
	insertLogFile  *sql.Stmt
	selectLogFile  *sql.Stmt
	db             *sql.DB
}

const createTable = "CREATE TABLE IF NOT EXISTS "
const dropTable = "DROP TABLE IF EXISTS "
const idField = "id BINARY(16) PRIMARY KEY"
const createLogFileTable = createTable + "LOGFILE (" + idField + ", filename VARCHAR(255), created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
const createLogURITable = createTable + "LOGURI (" + idField + ", uri VARCHAR(255), created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
const createLogReferrerTable = createTable + "LOGREFERRER (" + idField + ", uri VARCHAR(255), created TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
const createLogEntryTable = createTable + "LOGENTRY (" + idField + ", logfile_id INT, loguri_id INT, ipaddress varchar(16), clientident varchar(255), clientauth varchar(255), clientversion varchar(255), requestmethod VARCHAR(16), requestprotocol VARCHAR(16), size BIGINT, status INT, referrer VARCHAR(255))"
const createClientTable = createTable + "CLIENT ()"
const dropLogFileTable = dropTable + " LOGFILE"
const dropLogEntryTable = dropTable + " LOGENTRY"
const dropLogURITable = dropTable + " LOGURI"
const dropLogReferrerTable = dropTable + " LOGREFERRER"
const insertQuery = "INSERT INTO LOGENTRY(id, logfile_id, loguri_id, ipaddress, clientident, clientauth, clientversion, requestmethod, requestprotocol, size, status, referrer) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"
const insertLogFile = "INSERT INTO LOGENTRY(id, logfile_id, loguri_id, ipaddress, clientident, clientauth, clientversion, requestmethod, requestprotocol, size, status, referrer) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)"

func New(dbdriver string, dbconnection string) (*LogStore, error) {
	result := LogStore{}
	result.dbconnection = dbconnection
	result.dbdriver = dbdriver
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
	return nil
}

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
	s.selectLogFile, err = s.db.PrepareContext(ctx, "SELECT id FROM LOGFILE WHERE filename = ?")
	if err != nil {
		fmt.Println(err)
		return err
	}

	s.insertLogFile, err = s.db.PrepareContext(ctx, "INSERT INTO LOGFILE (id, filename, created) VALUES (?,?,?)")
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
	return
}

// LookupURI retrieves the file id of a log file
func (s *LogStore) LookupURI(uri string) (string, error) {
	return uuid.New().String(), nil
}

// LookupLogFile retrieves the file id of a log file
func (s *LogStore) LookupLogFile(logfile string) (string, error) {
	r := s.logfilecache[logfile]
	if r != "" {
		return r, nil
	}
	var id string
	err := s.selectLogFile.QueryRow(logfile).Scan(&id)
	if err != nil {
		if err == sql.ErrNoRows {
			id = uuid.New().String()
			_, err = s.insertLogFile.Exec(id, logfile, time.Now())
			if err != nil {
				log.Printf("insert err: %v", err)
				return "", err
			}
			s.logfilecache[logfile] = id
			return id, nil
		}
		log.Printf("select err: %v", err)
		return "", err
	}
	return id, nil
}

// LookupIPAddress retrieves the uuid for an ip address
func (s *LogStore) LookupIPAddress(ip string) (string, error) {
	return uuid.New().String(), nil
}

// LookupReferrer retrieves the referrer
func (s *LogStore) LookupReferrer(referrer string) (string, error) {
	return uuid.New().String(), nil
}

// Params is a map of arguments to sql
type Params map[string]interface{}

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
	fileID, err := s.LookupLogFile(entry.GetLogFile())

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
