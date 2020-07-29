package mysql

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"

	// Load the mysql driver
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/infodancer/implog/httplog"
)

type MysqlLogStore struct {
	dbdriver     string
	dbconnection string
	db           *sql.DB
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

func New(dbdriver string, dbconnection string) (*MysqlLogStore, error) {
	result := MysqlLogStore{}
	result.dbconnection = dbconnection
	result.dbdriver = dbdriver
	return &result, nil
}

// Clear drops the tables used for storing log data, normally so they can be recreated in a new format
func (s *MysqlLogStore) Clear(ctx context.Context) error {
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

func (s *MysqlLogStore) Open() error {
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
func (s *MysqlLogStore) Ping(ctx context.Context) error {
	if err := s.db.PingContext(ctx); err != nil {
		log.Fatal(err)
		return err
	}
	return nil
}

// Init creates the table structure for storing records, if necessary
func (s *MysqlLogStore) Init(ctx context.Context) error {
	fmt.Printf("Init()\n")
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
		return err
	}
	s.db.Begin()
	defer tx.Rollback()
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

	return nil
}

// Close closes the database connection
func (s *MysqlLogStore) Close() {
	return
}

// LookupURI retrieves the file id of a log file
func (s *MysqlLogStore) LookupURI(uri string) (string, error) {
	return uuid.New().String(), nil
}

// LookupLogFile retrieves the file id of a log file
func (s *MysqlLogStore) LookupLogFile(logfile string) (string, error) {
	return uuid.New().String(), nil
}

// LookupIPAddress retrieves the uuid for an ip address
func (s *MysqlLogStore) LookupIPAddress(ip string) (string, error) {
	return uuid.New().String(), nil
}

// LookupReferrer retrieves the referrer
func (s *MysqlLogStore) LookupReferrer(referrer string) (string, error) {
	return uuid.New().String(), nil
}

// Params is a map of arguments to sql
type Params map[string]interface{}

// WriteHTTPLogEntry writes an http log entry to the log store
func (s *MysqlLogStore) WriteHTTPLogEntry(ctx context.Context, entry httplog.Entry) error {
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

	insert, err := s.db.PrepareContext(ctx, insertQuery)
	defer insert.Close()

	_, err = insert.ExecContext(ctx, uuid, fileID, uriID, entry.GetIPAddress(), entry.GetClientIdent(),
		entry.GetClientAuth(), entry.GetClientVersion(), entry.GetRequestMethod(), entry.GetRequestProtocol(),
		entry.GetSize(), entry.GetStatus(), referrerID)
	if err != nil {
		log.Printf("error inserting %v: %v", uuid, err)
		return err
	}
	tx.Commit()

	return nil
}
