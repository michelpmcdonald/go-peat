package tsprovider

import (
	"github.com/michelpmcdonald/go-peat"
	"math/rand"
	"time"
)

// StaticTradesSource generates random timed trades starting at
// start time and stopping when the total number of records
// reaches TotalTrades
type StaticTradesSource struct {
	Symbol      string
	TotalTrades int
	count       int
	startTime   time.Time
	endTime     time.Time
	lastTime    time.Time
}

// Next implements an iterator for the contents of the csv data
func (st *StaticTradesSource) Next() (gopeat.TimeStamper, bool) {
	if st.startTime.IsZero() {
		panic("staticTradeSource: starttime not set")
	}
	if st.count < st.TotalTrades {
		st.count++
		st.lastTime = st.lastTime.Add(time.Nanosecond * time.Duration(rand.Intn(899999999)))
		return Trade{Tim: st.lastTime, Vol: 15, Amt: 3011.25}, true
	}
	
	return nil, false

}

// SetStartTime sets min timpstamp for data provided
func (st *StaticTradesSource) SetStartTime(startTime time.Time) {
	st.startTime = startTime
	st.lastTime = startTime
}

// SetEndTime sets max timpstamp for data provided
func (st *StaticTradesSource) SetEndTime(endTime time.Time) {
	st.endTime = endTime
}
