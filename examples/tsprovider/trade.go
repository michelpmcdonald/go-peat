package tsprovider

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/michelpmcdonald/go-peat"
)

// Trade is a sale of a security at a time
type Trade struct {
	Tim time.Time
	Vol int
	Amt float64
}

// GetTimeStamp returns the time the trade executed
func (trd Trade) GetTimeStamp() time.Time {
	return trd.Tim
}

// MarshalJSON returns version of trade with a unix timestamp
func (trd Trade) MarshalJSON() ([]byte, error) {
	type Alias Trade
	return json.Marshal(&struct {
		*Alias
		Time string `json:"stamp"`
	}{
		Alias: (*Alias)(&trd),
		Time:  fmt.Sprint(trd.Tim.UnixNano() / int64(time.Millisecond/time.Nanosecond)),
	})
}

const tdiTimeLayout string = "01/02/2006 15:04:05.999 MST"

// TdiCsvToTrd converts a csv line slice in
// TickData's (www.tickdata.com) format to a Trade Value
// Symbol,Date,Time,Price,Volume
// ESU13,09/01/2013,17:00:00.083,1640.25,8
func TdiCsvToTrd(csv []string) (gopeat.TimeStamper, error) {
	tim, _ := time.Parse(tdiTimeLayout, csv[1]+" "+csv[2]+" "+"UTC")
	amt, _ := strconv.ParseFloat(csv[3], 64)
	vol, _ := strconv.Atoi(csv[4])
	return Trade{
		Tim: tim,
		Amt: amt,
		Vol: vol,
	}, nil
}
