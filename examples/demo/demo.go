// A self contained simple demo for go-peat
package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/michelpmcdonald/go-peat"
)

// TsRec - any old struct will work as long
// as it has a time to use in playback
type tsRec struct {
	tsWeirdName time.Time
	amt         float64
}

// GetTimeStamp - your weird struct on gopeat
func (ts tsRec) GetTimeStamp() time.Time {
	return ts.tsWeirdName
}

// Now some data is needed, typically this would
// be from a csv file, but whatever suits ya
var tsCsv = `weird_name, amt
		    09/01/2013 17:00:00.083 UTC, 12
		    09/01/2013 17:00:00.088 UTC, 55
			09/01/2013 17:00:00.503 UTC, 54
			09/01/2013 17:00:03.201 UTC, 54`

// Define func to convert a csv line slice to our struct
func csvToTsRec(csv []string) (gopeat.TimeStamper, error) {
	tim, _ := time.Parse("01/02/2006 15:04:05.999 MST",
		strings.TrimSpace(csv[0]))
	amt, _ := strconv.ParseFloat(strings.TrimSpace(csv[1]), 64)
	return tsRec{tsWeirdName: tim, amt: amt}, nil
}

func main() {

	// A data source is needed. Since our demo data is csv, use
	// gopeats csv time stamper source and provide the data and
	// the csv line converter. It implements the needed interfaces.
	// Create your own source as needed, just implement
	// TimeBracket and TimeStampSource interfaces
	tsSource := &gopeat.CsvTsSource{
		Symbol:    "MES",
		CsvStream: strings.NewReader(tsCsv),
		CsvTsConv: csvToTsRec,
	}

	// PlayBack controls the sim run
	var sim *gopeat.PlayBack

	// Create a callback to handle data, called when the simulation time
	// reaches the data's timestamp, so it's in sim soft real time
	recCnt := 0
	dataOut := func(ts gopeat.TimeStamper) error {
		recCnt++
		if recCnt > 2 {
			// Stop the sim early, skip last record
			sim.Quit()
		}
		tsd := ts.(tsRec)
		fmt.Printf("Data Time: %v. Data Amt: %f\n",
			tsd.GetTimeStamp(),
			tsd.amt)
		return nil
	}

	//Create a playback
	sim, _ = gopeat.New(
		"MES",
		time.Date(2013, 9, 1, 17, 0, 0, 0, time.UTC), //Sim start time
		time.Date(2013, 9, 1, 17, 4, 0, 0, time.UTC), //Sim end time
		tsSource,
		2,       //Sim rate
		dataOut) //Call back

	// Start the replay
	sim.Play()

	// Block until done
	sim.Wait()
}
