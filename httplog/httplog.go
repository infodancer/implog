package httplog

import (
	"crypto/sha1"
	"errors"
	"strconv"
	"strings"
	"time"
)

// Entry represents a standard HTTP log format
type Entry struct {
	UUID            []byte
	isParseError    bool
	logtype         string
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

func (e *Entry) IsParseError() bool {
	return e.isParseError
}

func (e *Entry) GetLogType() string {
	return e.logtype
}

func (e *Entry) GetUUID() []byte {
	return e.UUID
}

func ParseLogLine(line string) (*Entry, error) {
	result := Entry{}

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
