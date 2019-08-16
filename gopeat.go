// Package gopeat provides functionality to replay time stamped data
// with BESRTA(best effort soft real time accuracy). Playback preserves
// time between consecutive time stamped data values so any time drifts
// will be accumulated over the total run time.
package gopeat

import (
	"container/list"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"
)

// TimeStamper is implemented by any value that has a GetTimeStamp
// method, which defines a time stamp for that value.
// The GetTimeStamp method is used by Playback as the point in time to
// provide the value to Playback clients
type TimeStamper interface {
	GetTimeStamp() time.Time
}

// TimeBracket is implemented by any value that has SetStartTime and
// a SetEndTime methods, which defines the time bracket the value falls
// in. Playback uses the interface to notify its time series data
// source to limit data to the given time bracket
type TimeBracket interface {
	SetStartTime(startTime time.Time)
	SetEndTime(startTime time.Time)
}

// TimeStampSource is implemented by any value that has a Next iterator
// method which returns TimeStamper values.  When ok is false iterator
// is past the last value and the previous Next call returned
// the last value.
type TimeStampSource interface {
	Next() (tsData TimeStamper, ok bool)
}

// OnTsDataReady is the function the Playback client should provide to
// the playback to receive the time stamped data at simulation time.
// The client implementation should return as soon as the time sensitive
// part of it's processing is complete in order to keep Playback's
// internal backpressure calculation accurate
type OnTsDataReady func(TimeStamper) error

// PlayBack implements a simulation run.  Playback clients need to
// provide a data source that implement both TimeBracket and
// TimeStampSource interfaces.  Clients can stop the playback
// by closing StopChan.
type PlayBack struct {
	Symbol        string
	StartTime     time.Time
	EndTime       time.Time
	SendTs        OnTsDataReady
	TsDataSource  TimeStampSource
	StopChan      chan struct{}
	PauseChan     chan struct{}
	SimRate       int16
	simRatDur     time.Duration
	tsDataChan    chan []TimeStamper
	tsDataChanLen int
	tsDataBufSize int
	timingsInfo   *list.List
	WallStartTime time.Time
}

// New allocates a new Playback struct
func New(symbol string,
	startTime time.Time, endTime time.Time,
	tsSource TimeStampSource,
	simRate int16,
	cb OnTsDataReady) (*PlayBack, error) {

	// Time stamped data source is required
	if tsSource == nil {
		return nil, errors.New("playBack: tsSource required")
	}
	pb := &PlayBack{
		Symbol:       symbol,
		StartTime:    startTime,
		EndTime:      endTime,
		TsDataSource: tsSource,
		SimRate:      simRate,
		SendTs:       cb}

	// No cb create one, just heating the room i guess
	if cb == nil {
		pb.SendTs = func(ts TimeStamper) error { return nil }
	}

	// Hard code right now to 500.  Indicates the number of
	// timestamper data values read from the timestamper source and
	// accumulated in a buffer beforethe buffer is written
	// to the data channel for sending
	pb.tsDataBufSize = 500

	// Hard code to 5 for right now. Indicates the size of the
	// buffered chan
	pb.tsDataChanLen = 5

	// Chan for incoming time stamped data from tsDataSource
	pb.tsDataChan = make(chan []TimeStamper, pb.tsDataChanLen)

	// Stop chan, call close to end simulation
	pb.StopChan = make(chan struct{})

	// Pause chan, write any value to pause simulation(not impl yet)
	pb.PauseChan = make(chan struct{})

	// Notify timestamper data source of playback start-end times
	pb.TsDataSource.(TimeBracket).SetStartTime(startTime)
	pb.TsDataSource.(TimeBracket).SetEndTime(endTime)

	// Set the simulation rate duration
	pb.simRatDur = time.Duration(pb.SimRate)

	return pb, nil
}

// loadTimeStampedData reads data from the source into a slice and
// then writes the slice to a chan.  A slice is used to reduce chan
// contention between this loader and the sender. A buffered chan is
// used so data can be preloaded and the sender does not have to block
// waiting for data. While the sender is consuming a slice of
// time stamped data from the chan, the loader reads ahead more data
// into a slice and writes the slice to the chan.
func (pb *PlayBack) loadTimeStampedData() {

	// Close the chan when done loading data
	defer close(pb.tsDataChan)

	// Buffer holds chunks of time stamped data
	tsDataBuf := make([]TimeStamper, 0, pb.tsDataBufSize)

	for {
		// Get the next time stamped data value from
		// the source until it is empty
		tsData, more := pb.TsDataSource.Next()
		if !more {
			break
		}

		select {
		default:
			tsDataBuf = append(tsDataBuf, tsData)

			// If the buffer slice is full, write it to the chan
			if len(tsDataBuf) == pb.tsDataBufSize {

				// sendBuf now refers to the buffers slice data
				sendBuf := tsDataBuf
				pb.tsDataChan <- sendBuf

				// buffer is reallocated to a new slice
				tsDataBuf = make([]TimeStamper, 0, pb.tsDataBufSize)
			}
		// Stop if StopChan is signaled
		case <-pb.StopChan:
			return
		}
	}
	// source is empty, send any remaining data in the buffer
	if len(tsDataBuf) > 0 {
		pb.tsDataChan <- tsDataBuf
	}
}

// Play starts replay process, blocks until run is complete
func (pb *PlayBack) Play() {

	// Wait group for send completion
	var wg sync.WaitGroup
	wg.Add(1)

	// Start loading timestamped data from time stamp source,
	// wait a few seconds to read-buffer-chan write some data
	go pb.loadTimeStampedData()
	time.Sleep(5 * time.Second)

	// Start sending the time stamp data back out at sim time
	wallStart := time.Now()
	go pb.send(&wg)

	wg.Wait()
	// Calc performance results
	wallEnd := time.Now()
	pb.TimeDrift(*pb.timingsInfo)
	fmt.Printf("Actual Run time: %f(s)\n",
		wallEnd.Sub(wallStart).Seconds())

}

// send outputs the timestamp data at simulation times. At the
// appropriate simulation time the client provided callback is invoked
// with the time stamped data.
func (pb *PlayBack) send(wg *sync.WaitGroup) {
	defer wg.Done()

	// A list has the constant insert time
	// that is needed in the timing loop
	pb.timingsInfo = list.New()

	// Total number of records sent
	var tsRecCnt int64

	// Running drift factor is a backpressure time adjustment that
	// takes into account the time client callbacks are taking to
	// complete. For example, a client callback that writes to a
	// network could slow down and speed up as the network load
	// changes.
	var driftFactor time.Duration

	// Wall simulation start time
	pb.WallStartTime = time.Now()

	// Sim timestamp of the previous tsData sent
	prevTsDataTime := pb.StartTime

	// Wall time of the prev tsData send
	prevWallSendTime := pb.WallStartTime

	// read next slice of time stamped data from chan
	for tsDataBuf := range pb.tsDataChan {

		// process each ts value from slice
		for _, tsData := range tsDataBuf {
			select {
			default:
				tsRecCnt++

				// time between this ts data and the prev ts data
				intervaleDur := tsData.GetTimeStamp().Sub(prevTsDataTime)

				// actual wall time between now and the time the prev
				// ts data value was sent out
				wallDur := time.Since(prevWallSendTime)

				// sleep duration is the diff between the time between
				// ts data values and the wall time since the prev
				// data value was sent.
				// For example, if the next ts data item is supposed to
				// go out 2 seconds after the prev data item, and it's
				// been .5 seconds since the prev item was sent, sleep
				// 1.5 seconds before sending to hit the 2 second mark.
				sd := (intervaleDur - wallDur) / pb.simRatDur

				// adjust sleep duration by drift factor to account for
				// last client callback time.
				time.Sleep(sd - driftFactor)

				// Client call back ie the send.
				// This is the time sensitive point of consumption
				pb.SendTs(tsData)
				wallSendTime := time.Now()

				// driftDur is actual wall time between sends minus the
				// time stamp calculated desired time between sends.
				// Drift can go negative due to the drift factor
				// causing the client send to happen to early.
				driftDur := wallSendTime.Sub(prevWallSendTime) -
					(intervaleDur / pb.simRatDur)

				// Collect timing data
				rt := runTimings{}
				rt.trdTime = tsData.GetTimeStamp()
				rt.sd = sd
				rt.recNum = tsRecCnt
				rt.driftDur = driftDur
				pb.timingsInfo.PushBack(rt)

				// Set up loop for next iteration
				prevWallSendTime = wallSendTime
				prevTsDataTime = tsData.GetTimeStamp()

				// Re-calc drift factor.
				// If the last send's drift was positive the client
				// callback took longer than expected. In this case the
				// drift factor is increased which causes the pre send
				// sleep duration to decrease. The shorter sleep
				// duration causes the client callback to get called
				// earlier to account for its lag.
				// A negative drift means the client callback was faster
				// than expected.  In this case the drift factor is
				// decreased, the pre send sleep duration is increased,
				// and the client callback gets called later.
				driftFactor = driftFactor + rt.driftDur

			case <-pb.StopChan:
				// stop the playback
				return
			case <-pb.PauseChan:
				// temp, add actual pause code here
				// Need a way to restart
				// Need a way to account for pause time for
				// next trade that goes out
			}
		}
	}
}

// runTimings holds timing info for each data value in the simulation
// run
type runTimings struct {
	trdTime             time.Time
	sd                  time.Duration
	realTimeBeforeSleep time.Time
	recNum              int64
	driftDur            time.Duration
}

// TimeDrift calculates some run time timing info
func (pb *PlayBack) TimeDrift(actSecs list.List) {
	maxDrift := 0.0
	var actSec runTimings
	for el := actSecs.Front(); el != nil; el = el.Next() {
		actSec = el.Value.(runTimings)

		maxDrift = math.Max(maxDrift, math.Abs(actSec.driftDur.Seconds()/1000))
	}
	fmt.Printf("Max Drift between: %f(ms)\n", maxDrift)
	fmt.Printf("Expected Real run time %f(s)\n",
		(actSec.trdTime.Sub(pb.StartTime) / pb.simRatDur).Seconds())
}
