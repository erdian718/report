package report

import (
	"time"

	"github.com/ofunc/dt"
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
