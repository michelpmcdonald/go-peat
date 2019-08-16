// Very simple example of how create a gopeat Playback simulation
// run.
package main

import (
	"fmt"
	"math"
	"os"
	"time"

	"github.com/michelpmcdonald/go-peat"
	"github.com/michelpmcdonald/go-peat/examples/tsprovider"
)

var simStart = time.Date(2013, 9, 3, 0, 0, 0, 0, time.UTC)
var simEnd = time.Date(2013, 9, 3, 15, 59, 59, 999, time.UTC)
var simRate = int16(5000)
var simDurRate = time.Duration(simRate)

var wallStartTime *time.Time

var maxTimeSlip = 0.0

var cumPrice float64
var totalTrades int64
var cumVol int64

// Create a callback function for the simulation to call
// when a timestamp value is sent.
var dataOut = func(ts gopeat.TimeStamper) error {

	// time stamp source provides Trades(that
	// implement TimeStamper interface)
	trd := ts.(tsprovider.Trade)

	// wall duration time simulation has been running
	wallDur := time.Since(*wallStartTime)

	// expected wall duration. For example, if the data should appear
	// 30 seconds after the start time and the sime is running at 2x
	// expect that callback at 15 seconds wall time into the sim run
	expDur := ts.GetTimeStamp().Sub(simStart) / simDurRate

	maxTimeSlip = math.Max(math.Abs((wallDur - expDur).Seconds()),
		maxTimeSlip)

	cumPrice += float64(trd.Amt)
	cumVol += int64(trd.Vol)
	totalTrades++
	if totalTrades%5000 == 0 {
		fmt.Printf("Processing rec %d\n", totalTrades)
		fmt.Println(ts)
	}
	return nil
}

func main() {

	// Create sim symbol, simulation start and end times
	sym := "mes"

	// Open a trades csv file for the
	fn := "./examples/tsprovider/ES_Trades.csv"
	csvFile, errFile := os.Open(fn)
	if errFile != nil {
		panic(errFile)
	}
	defer csvFile.Close()

	// Create a Time Stamp Source
	tsSource := &gopeat.CsvTsSource{
		Symbol:    sym,
		CsvStream: csvFile,
		CsvTsConv: tsprovider.TdiCsvToTrd}
	tsSource.MaxRecs = 2000000

	// Create a new simulation playback, inject the tradesource
	sim, err := gopeat.New(
		sym,
		simStart,
		simEnd,
		tsSource,
		simRate,
		dataOut) //Call back
	if err != nil {
		return
	}

	wallStartTime = &sim.WallStartTime

	// Start the playback
	sim.Play()

	fmt.Printf("Vwap: %f\n", cumPrice/float64(cumVol))
	fmt.Printf("total vol %d\n", cumVol)
	fmt.Printf("Max time slip: %f(seconds)\n", maxTimeSlip)
}
