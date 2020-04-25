package dsio

import (
	"fmt"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore"
)

func MarshalKey(key *datastore.Key) string {
	var s string
	if key.Name == "" {
		s = fmt.Sprintf("/%s,%v", key.Kind, key.ID)
	} else {
		digits := len(key.Name) > 0
		for i, n := 0, len(key.Name); i < n && i < 5 && digits; i++ {
			if key.Name[i] < '0' || key.Name[i] > '9' {
				digits = false
			}
		}
		name := escapeKeyPart(key.Name)
		if digits {
			name = "`" + name
		}
		s = fmt.Sprintf("/%s,%v", key.Kind, name)
	}
	if key.Namespace != "" {
		s = s + "`" + escapeKeyPart(key.Namespace)
	}
	if key.Parent != nil {
		s = MarshalKey(key.Parent) + s
	}
	return s
}

func escapeKeyPart(s string) string {
	if !strings.ContainsAny(s, "`^/") {
		return s
	}
	return strings.Replace(strings.Replace(strings.Replace(strings.Replace(s, `^`, `^^`, -1), `,`, `^,`, -1), `/`, `^/`, -1), "`", "^`", -1)
}

func UnmarshalKey(s string) (key *datastore.Key) {
	b := []byte(s)
	quote := false
	mode := 0
	var buf []byte
	kind, value := "", ""
	inttype := true
	for i, n := 0, len(b); i < n; i++ {
		if quote {
			quote = false
		} else {
			switch b[i] {
			case '^':
				quote = true
				continue
			case '/':
				if mode > 1 {
					if mode == 2 {
						value = string(buf)
					}
					if inttype {
						id, _ := strconv.ParseInt(value, 10, 64)
						key = datastore.IDKey(kind, id, key)
					} else {
						key = datastore.NameKey(kind, value, key)
					}
					if mode == 3 {
						key.Namespace = string(buf)
					}
					kind, value, buf, inttype = "", "", nil, true
				}
				mode = 1
				continue
			case ',':
				if mode == 1 {
					kind = string(buf)
					buf = nil
					mode = 2
					continue
				}
			case '`':
				if mode == 2 {
					if len(buf) == 0 {
						inttype = false
					} else {
						mode = 3
						value = string(buf)
						buf = nil
					}
					continue
				}
			}
		}
		buf = append(buf, b[i])
		if mode == 2 && inttype && (b[i] < '0' || b[i] > '9') {
			inttype = false
		}
	}
	if mode > 1 {
		if mode == 2 {
			value = string(buf)
		}
		if inttype {
			id, _ := strconv.ParseInt(value, 10, 64)
			key = datastore.IDKey(kind, id, key)
		} else {
			key = datastore.NameKey(kind, value, key)
		}
		if mode == 3 {
			key.Namespace = string(buf)
		}
	}
	return
}
