package gopeat

import (
	"encoding/csv"
	"io"
	"time"
)

// CsvToTs converts a csv line slice to a TimeStamper value
type CsvToTs func([]string) (TimeStamper, error)

// CsvTsSource implement a time stamped data source for
// csv data(with header). Client must provide CsvToTs to
// convert csv data to timestamper value
type CsvTsSource struct {
	Symbol    string
	CsvStream io.Reader
	CsvTsConv CsvToTs
	csvReader *csv.Reader
	startTime time.Time
	endTime   time.Time
	recCount  int64
	MaxRecs   int64
}

// Next implements an iterator for the contents of the csv data
func (st *CsvTsSource) Next() (TimeStamper, bool) {
	if st.startTime.IsZero() {
		panic("staticTradeSource: starttime not set")
	}
	if st.csvReader == nil {
		st.csvReader = csv.NewReader(st.CsvStream)
		_, _ = st.csvReader.Read()
	}
	var trd TimeStamper
	for {
		line, err := st.csvReader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}

		trd, _ = st.CsvTsConv(line)

		if trd.GetTimeStamp().Before(st.startTime) {
			continue
		}

		if trd.GetTimeStamp().After(st.endTime) {
			break
		}

		st.recCount++
		//early out if max recs count hit
		if st.recCount == st.MaxRecs {
			break
		}
		return trd, true

	}
	return nil, false

}

// SetStartTime sets min timpstamp for data provided
func (st *CsvTsSource) SetStartTime(startTime time.Time) {
	st.startTime = startTime
}

// SetEndTime sets max timpstamp for data provided
func (st *CsvTsSource) SetEndTime(endTime time.Time) {
	st.endTime = endTime
}
