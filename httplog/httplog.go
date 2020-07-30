package httplog

import (
	"crypto/sha1"
	"errors"
	"strconv"
	"strings"
	"time"
)

// EntryData represents a standard HTTP log format
type EntryData struct {
	UUID            []byte
	isParseError    bool
	logtype         string
	logfile         string
	IPAddress       string
	ClientIdent     string
	ClientAuth      string
	Timestamp       time.Time
	URL             string
	Status          int64
	Size            int64
	Referrer        string
	RequestMethod   string
	RequestURI      string
	RequestProtocol string
	RequestParams   string
	ClientVersion   string
}

// Entry defines the interface for HTTP log entries
type Entry interface {
	IsParseError() bool
	GetLogType() string
	GetLogFile() string
	SetLogFile(file string)
	GetUUID() []byte
	GetIPAddress() string
	GetClientIdent() string
	GetClientAuth() string
	GetClientVersion() string
	GetRequestMethod() string
	GetRequestProtocol() string
	GetRequestURI() string
	GetStatus() int64
	GetSize() int64
	GetReferrer() string
}

func (e *EntryData) IsParseError() bool {
	return e.isParseError
}

func (e *EntryData) GetLogType() string {
	return e.logtype
}

func (e *EntryData) GetLogFile() string {
	return e.logfile
}

func (e *EntryData) SetLogFile(file string) {
	e.logfile = file
}

func (e *EntryData) GetUUID() []byte {
	return e.UUID
}

func (e *EntryData) GetIPAddress() string {
	return e.IPAddress
}

func (e *EntryData) GetClientIdent() string {
	return e.ClientIdent
}

func (e *EntryData) GetClientAuth() string {
	return e.ClientAuth
}

func (e *EntryData) GetClientVersion() string {
	return e.ClientVersion
}

func (e *EntryData) GetRequestMethod() string {
	return e.RequestMethod
}

func (e *EntryData) GetRequestProtocol() string {
	return e.RequestProtocol
}

func (e *EntryData) GetRequestURI() string {
	return e.RequestURI
}

func (e *EntryData) GetStatus() int64 {
	return e.Status
}

func (e *EntryData) GetSize() int64 {
	return e.Size
}

func (e *EntryData) GetReferrer() string {
	return e.Referrer
}

func ParseLogLine(line string) (*EntryData, error) {
	result := EntryData{}
	result.isParseError = true
	// Hash the line for UUID to avoid duplicates
	bytes := []byte(line)
	hasher := sha1.New()
	hasher.Write(bytes)
	result.UUID = hasher.Sum(nil)

	words, err := parseEntryWords(line)
	if err != nil {
		return nil, err
	}
	result.isParseError = true
	if len(words) >= 1 {
		result.IPAddress = words[0]
	}
	if len(words) >= 2 {
		result.ClientIdent = words[1]
	}
	if len(words) >= 3 {
		result.ClientAuth = words[2]
	}
	if len(words) >= 5 {
		result.Timestamp, err = parseHTTPTimestamp(words[3])
		if err != nil {
			return nil, err
		}
	}
	if len(words) >= 5 {
		result.RequestMethod, err = parseRequestMethod(words[4])
		if err != nil {
			return nil, err
		}

		result.RequestURI, err = parseRequestURI(words[4])
		if err != nil {
			return nil, err
		}

		result.RequestParams, err = parseRequestParams(words[4])
		if err != nil {
			return nil, err
		}

		result.RequestProtocol, err = parseRequestProtocol(words[4])
		if err != nil {
			return nil, err
		}
	}
	if len(words) >= 6 {
		result.Status, err = strconv.ParseInt(words[5], 0, 64)
	}
	if len(words) >= 7 {
		result.Size, err = strconv.ParseInt(words[6], 0, 64)
	}
	if len(words) >= 8 {
		result.Referrer = words[7]
	}
	if len(words) >= 9 {
		result.ClientVersion = words[8]
	}
	result.isParseError = false
	result.logtype = "HTTP"
	return &result, nil
}

func parseHTTPTimestamp(word string) (time.Time, error) {
	return time.Parse("_2/Jan/2006:15:04:05 -0700", word)
}

func parseRequestMethod(request string) (string, error) {
	words, err := parseEntryWords(request)
	if err != nil {
		return "", err
	}
	if len(words) >= 1 {
		return words[0], nil
	}
	return "", errors.New("request method not specified")
}

func parseRequestURI(request string) (string, error) {
	words, err := parseEntryWords(request)
	if err != nil {
		return "", err
	}
	if len(words) >= 2 {
		return words[1], nil
	}
	return "", errors.New("request protocol not specified")
}

func parseRequestParams(request string) (string, error) {
	words, err := parseEntryWords(request)
	if err != nil {
		return "", err
	}
	if len(words) >= 2 {
		uri := strings.Split(words[1], "?")
		if len(uri) > 1 {
			return uri[1], nil
		}
	}
	return "", nil
}

func parseRequestProtocol(request string) (string, error) {
	words, err := parseEntryWords(request)
	if err != nil {
		return "", err
	}
	if len(words) >= 3 {
		return words[2], nil
	}
	return "", errors.New("request protocol not specified")
}

func parseEntryWords(line string) ([]string, error) {
	words := make([]string, 0)
	var word strings.Builder
	quoted := false
	for _, c := range line {
		s := string(c)
		if s == "\"" {
			if quoted {
				quoted = false
				words = append(words, word.String())
				word.Reset()
			} else {
				quoted = true
			}
		} else if s == "[" || s == "]" {
			if quoted {
				quoted = false
				words = append(words, word.String())
				word.Reset()
			} else {
				quoted = true
			}
		} else if s == " " && !quoted {
			if word.Len() > 0 {
				v := strings.TrimSpace(word.String())
				words = append(words, v)
			}
			word.Reset()
		} else {
			word.WriteString(s)
		}
	}
	if word.Len() > 0 {
		v := strings.TrimSpace(word.String())
		words = append(words, v)
	}
	return words, nil
}
