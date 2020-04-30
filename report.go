package report

import (
	"errors"
	"path/filepath"
	"time"

	"github.com/ofunc/dt"
	"github.com/ofunc/dt/io/csv"
	"github.com/ofunc/dt/io/xlsx"
)

// Report is the report entity.
type Report struct {
	path     string
	feed     func(name string) (time.Time, *dt.Frame, error)
	group    func(frame *dt.Frame) *dt.Frame
	sdate    time.Time
	edate    time.Time
	base     *dt.Frame // CODE[, NAME], SUPER, TARGET
	data     *dt.Frame // CODE, 20060102, ...
	schedule *dt.Frame // DATE, VALUE
	adjust   *dt.Frame // DATE, CODE[, NAME], VALUE
}

// Load loads the report.
func Load(path string, feed func(string) (time.Time, *dt.Frame, error), group func(*dt.Frame) *dt.Frame) (*Report, error) {
	cr, xr := csv.NewReader(), xlsx.NewReader()

	base, err := xr.ReadFile(filepath.Join(path, "base.xlsx"))
	if err != nil {
		return nil, err
	}
	if err := base.Check("CODE", "SUPER", "TARGET"); err != nil {
		return nil, err
	}
	base.Get("CODE").String()
	base.Get("SUPER").String()
	base.Get("TARGET").Number()

	data, err := cr.ReadFile(filepath.Join(path, "data.csv"))
	if err != nil {
		return nil, err
	}
	if err := data.Check("CODE"); err != nil {
		return nil, err
	}
	data.Get("CODE").String()
	data = base.Pick("CODE").Rename("CODE", "BCODE").
		Join(data, "CODE").On("BCODE").Do("").
		Del("CODE").Rename("BCODE", "CODE").
		FillNA(dt.Number(0))

	schedule, err := xr.ReadFile(filepath.Join(path, "schedule.xlsx"))
	if err != nil {
		return nil, err
	}
	if err := schedule.Check("DATE", "VALUE"); err != nil {
		return nil, err
	}
	schedule.Sort(func(x, y dt.Record) bool {
		return x.String("DATE") < y.String("DATE")
	}).FillNA(dt.Number(1), "VALUE")

	dates := schedule.Get("DATE")
	ndates := len(dates)
	if ndates < 2 {
		return nil, errors.New("report: invalid schedule frame")
	}
	sdate, err := ParseDate(dates[0].String())
	if err != nil {
		return nil, err
	}
	edate, err := ParseDate(dates[ndates-1].String())
	if err != nil {
		return nil, err
	}

	adjust, err := xr.ReadFile(filepath.Join(path, "adjust.xlsx"))
	if err != nil {
		return nil, err
	}
	if err := adjust.Check("DATE", "CODE", "VALUE"); err != nil {
		return nil, err
	}
	adjust.Get("CODE").String()

	return &Report{
		path:     path,
		feed:     feed,
		group:    group,
		sdate:    sdate,
		edate:    edate,
		base:     base,
		data:     data,
		schedule: schedule,
		adjust:   adjust,
	}, nil
}
