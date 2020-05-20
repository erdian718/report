package report

import (
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ofunc/dt"
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

// Group is the default group function.
func Group(base, data *dt.Frame) *dt.Frame {
	data = base.Join(data, "ID").Do("D_").
		Pick("ID", "LEVEL", "SUPER", "D_VALUE").
		Rename("D_VALUE", "VALUE").
		FillNA(dt.Number(0), "VALUE")

	for l := dt.Max(data.Get("LEVEL")).Number(); l >= 0; l-- {
		t := data.Filter(func(r dt.Record) bool {
			return r.Number("LEVEL") == l
		}).GroupBy("SUPER").Apply("VALUE", "SUM", dt.Sum).Do()

		data = data.Join(t, "SUPER").On("ID").Do("").
			FillNA(dt.Number(0), "SUM").
			MapTo("VALUE", func(r dt.Record) dt.Value {
				return dt.Number(r.Number("VALUE") + r.Number("SUM"))
			}).Pick("ID", "LEVEL", "SUPER", "VALUE")
	}
	return data.Pick("ID", "VALUE")
}

func bakup(name string) (string, error) {
	src, err := os.Open(name)
	if err != nil {
		return "", err
	}
	defer src.Close()

	target := filepath.Join(filepath.Dir(name), time.Now().Format("20060102150405.bak"))
	dst, err := os.Create(target)
	if err != nil {
		return "", err
	}
	defer dst.Close()

	if _, err := io.Copy(dst, src); err != nil {
		return "", err
	}
	return target, dst.Sync()
}
