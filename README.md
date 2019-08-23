# go-peat : A Golang library for replaying time-stamped data

## Overview [![GoDoc](https://godoc.org/github.com/michelpmcdonald/go-peat?status.svg)](https://godoc.org/github.com/michelpmcdonald/go-peat) [![Build Status](https://travis-ci.org/michelpmcdonald/go-peat.svg?branch=master)](https://travis-ci.org/michelpmcdonald/go-peat) [![Go Report Card](https://goreportcard.com/badge/github.com/michelpmcdonald/go-peat)](https://goreportcard.com/report/github.com/michelpmcdonald/go-peat)

## Motivation and Example Uses

I've needed common replay capability in several projects. Gopeat(re-peat idk...) was created as a general replay library(DRY) designed to be used with a time-stamped sorted data stream that can be represented as a golang struct with a time.Time getter.

Some Examples:

Equities Market: historic tick data chart replay for trader training

Live data stream proxy for development-testing of soft real-time trading systems, live dash boards and live GIS gps tracking data feeds.

Distributed Interactive Simulation(DIS): After Action Review(AAR) GIS overlay replay of DIS packets recorded during training and experimentation with Soldiers using new equipment and tactics.



## Tech details

Internally gopeat runs three goroutines: 

* Controller: responsible for starting the data loader, starting the timed data writer, and responding to Api control messages like Quit(), Pause()
Resume(). 

* Data Loader: a buffered read-ahead data stream fetcher that loads the
source playback data

* Data Timer: provides the timestamped data at the proper replay time
accounting for the sim rate

The callback model seemed to offer the most flexibility without exposing channels and WaitGroups(Avoid concurrency in your API - [Go Best Practices](https://talks.golang.org/2013/bestpractices.slide#25)
)

A back-pressure adjustment internally monitors the amount of time the client callback spends before returning and dynamically makes timing adjustments.  For example, if callbacks are taking 15 milliseconds callbacks will be executed 15 milliseconds earlier that the exact replay time derived from the time-stamped data in an attempt to "lead" the client.

The client should return from the callback as fast as possible to 
maximize replay time accuracy.  In some situations the client may not be
able to keep up and might need to implement a strategy like queuing up
 data or dropping data.

## Usage Notes
In order to utilize, the client should:

*Create a time stamp source struct that implements the gopeat.TimeStamper(name inspired by golang [fmt.Stringer](https://golang.org/pkg/fmt/#Stringer))

*Create a time stamper data source that implements gopeat.TimeBracket interface(sets replay start and end time) and implements gopeat.TimeStampSource.Next() iterator like interface that provides
the "TimeStamper" structs from the above step.
gopeat.CsvTsSource is a provided stamper data source for data stored
in Csv format. Use that directly or view the source to get an idea of how to implement your own source.

*Create a callback func that matches the gopeat.OnTsDataReady func type.

3 steps(2.5 if gopeat.CsvTsSource is used) to get going




## Install

```
go get github.com/michelpmcdonald/go-peat
```

## Example

```go
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


```

## Author

Michel McDonald
