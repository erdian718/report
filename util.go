package report

import (
	"time"
)

// Date return the date by time.
func Date(t time.Time) time.Time {
	y, m, d := t.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.Local)
}

// ParseDate parses v to date.
func ParseDate(v string) (time.Time, error) {
	return time.ParseInLocation("20060102", v, time.Local)
}

// FormatDate formats date to string.
func FormatDate(t time.Time) string {
	return Date(t).Format("20060102")
}
