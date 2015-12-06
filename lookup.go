package sqlite3

import (
	"fmt"

	"github.com/rs/rest-layer/resource"
	"github.com/rs/rest-layer/schema"
	"time"
	"strings"
)

// getQuery returns the WHERE clause when given a Lookup
func getQuery(l *resource.Lookup) (string, error) {
	return translateQuery(l.Filter())
}

// getSort returns the ORDER BY clause when given a Lookup
func getSort(l *resource.Lookup) string {
	return translateSort(l.Sort())
}

// translateQuery constructs the string representation of the WHERE clause of a SQL query
func translateQuery(q schema.Query) (string, error) {
	var str string
	for _, exp := range q {
		switch t := exp.(type) {
		case schema.And:
			var s string
			for _, subExp := range t {
				sb, err := translateQuery(schema.Query{subExp})
				if err != nil {
					return "", err
				}
				s += sb + " AND "
			}
			// remove the last " AND "
			str += "(" + s[:len(s)-5] + ")"
		case schema.Or:
			var s string
			for _, subExp := range t {
				sb, err := translateQuery(schema.Query{subExp})
				if err != nil {
					return "", err
				}
				s += sb + " OR "
			}
			// remove the last " OR "
			str += "(" + s[:len(s)-4] + ")"
		case schema.In:
			v, err := valuesToString(t.Values)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " IN (" + v + ")"
		case schema.NotIn:
			v, err := valuesToString(t.Values)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " NOT IN (" + v + ")"
		case schema.Equal:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			switch t.Value.(type) {
			case string:
				v = strings.Replace(v, "*", "%", -1)
				v = strings.Replace(v, "_", "\\_", -1)
				str += t.Field + " LIKE " + v + " ESCAPE '\\'"
			default:
				str += t.Field + " IS " + v
			}
		case schema.NotEqual:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			switch t.Value.(type) {
			case string:
				v = strings.Replace(v, "*", "%", -1)
				v = strings.Replace(v, "_", "\\_", -1)
				str += t.Field + " NOT LIKE " + v + " ESCAPE '\\'"
			default:
				str += t.Field + " IS NOT " + v
			}
		case schema.GreaterThan:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " > " + v
		case schema.GreaterOrEqual:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " >= " + v
		case schema.LowerThan:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " < " + v
		case schema.LowerOrEqual:
			v, err := valueToString(t.Value)
			if err != nil {
				return "", resource.ErrNotImplemented
			}
			str += t.Field + " <= " + v
		default:
			return "", resource.ErrNotImplemented
		}
	}
	return str, nil
}

// translateSort constructs the string representation of the ORDER BY clause of a SQL query
func translateSort(l []string) string {
	var str string
	if len(l) == 0 {
		return "id"
	}
	for _, s := range l {
		if string([]rune(s)[0]) == "-" {
			str += s[1:] + " DESC"
		} else {
			str += s
		}
		str += ","
	}
	return str[:len(str)-1]
}

// valuesToString combines a list of Values into a single comma separated string
func valuesToString(v []schema.Value) (string, error) {
	var str string
	for _, v := range v {
		s, err := valueToString(v)
		if err != nil {
			return "", err
		}
		str += fmt.Sprintf("%s,", s)
	}
	return str[:len(str)-1], nil
}

// valueToString converts a Value into a type-specific string
func valueToString(v schema.Value) (string, error) {
	var str string
	var i interface{} = v

	switch i.(type) {
	case int:
		str += fmt.Sprintf("%v", i)
	case float64:
		str += fmt.Sprintf("%v", i)
	case bool:
		str += fmt.Sprintf("%v", i)
	case string:
		str += fmt.Sprintf("'%v'", i)
	case time.Time:
		str += fmt.Sprintf("'%v'", i)
	default:
		return "", resource.ErrNotImplemented
	}
	return str, nil
}
