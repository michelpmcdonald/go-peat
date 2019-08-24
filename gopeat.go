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
// is past the last value and the previous Next call returned the last
// value.  The way playback is currently designed, a implementor of
// TimeStampSource should have a complete stream of data available so
// next can either return the next value or return EOF.  If Next()
// blocks, the loader goroutine will block and it will never
// terminate on it's own.  This design will be revisited.(TODO)
type TimeStampSource interface {
	Next() (tsData TimeStamper, ok bool)
}

// OnTsDataReady is the function the Playback client should provide to
// the playback to receive the time stamped data at simulation time.
// The client implementation should return as soon as the time sensitive
// part of it's processing is complete in order to keep Playback's
// internal backpressure calculation accurate. OnTsDataReady runs on
// Playback's send thread, not the clients thread
type OnTsDataReady func(TimeStamper) error

// PlayBack implements a simulation run.  Playback clients need to
// provide a data source that implement both TimeBracket and
// TimeStampSource interfaces.  Clients can stop the playback
// by closing StopChan.
type PlayBack struct {
	Symbol       string
	StartTime    time.Time
	EndTime      time.Time
	SendTs       OnTsDataReady
	TsDataSource TimeStampSource
	WallRunDur   time.Duration

	// Client specifies rate Ex: 2 = 2x, store it as
	// a duration for actual time use
	SimRate   int16
	simRatDur time.Duration

	// Source-Sender TimeStamper Data
	tsDataChan    chan []TimeStamper
	tsDataChanLen int
	tsDataBufSize int

	// Sim timed output
	timedTs chan TimeStamper

	// API Control chans
	quitChan   chan struct{}
	pauseChan  chan struct{}
	resumeChan chan struct{}

	// API Control flags to support
	// idempotent play-pause-resume-quit
	paused       bool
	replayActive bool

	// Keep track of pause time, set to 0 after using
	pauseDur time.Duration
	pauseMu  sync.RWMutex

	controllerStarted sync.WaitGroup

	// Holds run time timing info for reporting
	timingsInfo *list.List

	controllerStopped sync.WaitGroup

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
	// accumulated in a buffer before the buffer is written
	// to the data channel for sending
	pb.tsDataBufSize = 500

	// Hard code to 5 for right now. Indicates the size of the
	// buffered chan
	pb.tsDataChanLen = 5

	// Notify timestamper data source of playback start-end times
	pb.TsDataSource.(TimeBracket).SetStartTime(startTime)
	pb.TsDataSource.(TimeBracket).SetEndTime(endTime)

	// Set the simulation rate duration
	pb.simRatDur = time.Duration(pb.SimRate)

	return pb, nil
}

// init prepare for new run
func (pb *PlayBack) init() {
	pb.tsDataChan = make(chan []TimeStamper, pb.tsDataChanLen)
	pb.timedTs = make(chan TimeStamper)

	pb.pauseChan = make(chan struct{})
	pb.resumeChan = make(chan struct{})
	pb.quitChan = make(chan struct{})

	pb.paused = false
	pb.replayActive = false
	pb.timingsInfo = nil
}

// Play starts replay process
func (pb *PlayBack) Play() {
	if !pb.replayActive {
		// Start up the controller, controller starts and controls
		// the replay
		pb.controllerStopped.Add(1)
		pb.controllerStarted.Add(1)
		go pb.controller()
		pb.controllerStarted.Wait()
	}
}

// Pause suspends the running replay
func (pb *PlayBack) Pause() {
	if !pb.paused && pb.replayActive {
		// Open up the resume chan to allow ending the
		// pause which is being initiated here
		pb.resumeChan = make(chan struct{})

		// Send pause signal
		close(pb.pauseChan)
		pb.paused = true
	}
}

// Resume continues a paused playback
func (pb *PlayBack) Resume() {
	if pb.paused {

		// Open up the pause chan to allow pausing the
		// playback which is being restarted here
		pb.pauseChan = make(chan struct{})

		// Send resume signal
		close(pb.resumeChan)
		pb.paused = false
	}
}

// Quit stops the running PlayBack and eventually unblocks callers
// blocked on Wait()
func (pb *PlayBack) Quit() {
	if pb.replayActive {
		close(pb.quitChan)
		pb.replayActive = false
	}
}

// Wait blocks until the sender shuts down
func (pb *PlayBack) Wait() {
	pb.controllerStopped.Wait()
	pb.replayActive = false
}

// loadTimeStampedData reads data from the source into a slice and
// then writes the slice to a chan.  A slice is used to reduce chan
// contention between this loader and the sender. A buffered chan is
// used so data can be preloaded and the sender does not have to block
// waiting for data. While the sender is consuming a slice of
// time stamped data from the chan, the loader reads ahead more data
// into a slice and writes the slice to the chan.
func (pb *PlayBack) loadTimeStampedData() {

	defer close(pb.tsDataChan)

	tsDataBuf := make([]TimeStamper, 0, pb.tsDataBufSize)

	for {
		tsData, more := pb.TsDataSource.Next()
		if !more {
			break
		}

		// Stop if quit is signaled
		select {
		case <-pb.quitChan:
			return
		default:
		}

		tsDataBuf = append(tsDataBuf, tsData)

		// If the buffer slice is full, write it to the chan
		if len(tsDataBuf) == pb.tsDataBufSize {

			// sendBuf now refers to the buffers slice data
			sendBuf := tsDataBuf
			pb.tsDataChan <- sendBuf

			// buffer is reallocated to a new slice
			tsDataBuf = make([]TimeStamper, 0, pb.tsDataBufSize)
		}
	}
	// source is empty, send any remaining data in the buffer
	if len(tsDataBuf) > 0 {
		pb.tsDataChan <- tsDataBuf
	}
}

// controller starts a new playback run and handles PlayBack API
// commands. Blocks, but never sleeps. Terminates when there is no
// more data or an API command stops it
func (pb *PlayBack) controller() {
	defer pb.controllerStopped.Done()
	defer func() { pb.WallRunDur = time.Since(pb.WallStartTime) }()

	// Start with a clean slate
	pb.init()
	pb.replayActive = true

	// Start loading timestamped data from time stamp source,
	// wait a few seconds to fill up read ahead buffers
	go pb.loadTimeStampedData()
	time.Sleep(1 * time.Second)

	// Start the timed data producer
	go pb.dataTimer()

	// Wall simulation start time
	pb.WallStartTime = time.Now()

	pb.controllerStarted.Done()

	for {
		select {
		// data comes in at sim time on
		// timedTs chan.
		case tsData, ok := <-pb.timedTs:
			if !ok {
				// All data has been sent
				return
			}
			// Client supplied callback
			pb.SendTs(tsData)
		case <-pb.quitChan:
			return
		case <-pb.pauseChan:
			pWallStart := time.Now()
			select {
			case <-pb.resumeChan:
				pb.pauseMu.Lock()
				pb.pauseDur = time.Since(pWallStart) + pb.pauseDur
				pb.pauseMu.Unlock()
			case <-pb.quitChan:
				return
			}
		}
	}
}

// dataTimer outputs the timestamp data at simulation time on the
// timedTs chan
func (pb *PlayBack) dataTimer() {
	defer close(pb.timedTs)

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

	// Sim timestamp of the previous tsData sent
	prevTsDataTime := pb.StartTime

	// Wall time of the prev tsData send
	prevWallSendTime := time.Now()

	// read next slice of time stamped data from chan
	for tsDataBuf := range pb.tsDataChan {
		for _, tsData := range tsDataBuf {
			tsRecCnt++
		SleepCheck:
			select {
			case <-pb.quitChan:
				// stop the playback
				return

			case <-pb.pauseChan:
				select {
				case <-pb.resumeChan:
				case <-pb.quitChan:
					return
				}
			default:
			}

			// No need to run timing calcs for repeated timestamps
			var sd time.Duration
			var tsDur time.Duration
			if !tsData.GetTimeStamp().Equal(prevTsDataTime) {

				// time between this ts data and the prev ts data
				// adjusted for sim rate TODO rename tsDur
				tsDur = tsData.GetTimeStamp().Sub(prevTsDataTime)
				tsDur = tsDur / pb.simRatDur

				// actual wall time between now and the time the prev
				// ts data value was sent out
				pb.pauseMu.RLock()
				wallDur := time.Since(prevWallSendTime) - pb.pauseDur
				pb.pauseMu.RUnlock()

				// sleep duration is the diff between the time between
				// ts data values and the wall time since the prev
				// data value was sent.
				// For example, if the next ts data item is supposed to
				// go out 2 seconds after the prev data item, and it's
				// been .5 seconds since the prev item was sent, sleep
				// 1.5 seconds before sending to hit the 2 second mark.
				sd = (tsDur - wallDur) - driftFactor

				// Only sleep up to 250 ms at a time so this method
				// can continue to respond to API signals, otherwise the
				// longest sleep duration is data driven and unbounded
				if sd > (250 * time.Millisecond) {
					time.Sleep(250 * time.Millisecond)
					goto SleepCheck
				}
				time.Sleep(sd)
			}

			// Client call back ie the send.
			// This is the time sensitive point of consumption.
			// The whole point. Pièce de résistance
			//pb.SendTs(tsData)
			pb.timedTs <- tsData
			wallSendTime := time.Now()

			// driftDur is actual wall time between sends minus the
			// time stamp calculated desired time between sends.
			// Drift can go negative due to the drift factor
			// causing the client send to happen to early.
			pb.pauseMu.Lock()
			driftDur := (wallSendTime.Sub(prevWallSendTime) - pb.pauseDur) -
				(tsDur)

			// reset pause duration, done with pause adjustments
			pb.pauseDur = time.Duration(0 * time.Second)
			pb.pauseMu.Unlock()

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
		}
	}
}

// runTimings holds timing info for each timestamper
// that was emitted during the last playback run
type runTimings struct {
	trdTime             time.Time
	sd                  time.Duration
	realTimeBeforeSleep time.Time
	recNum              int64
	driftDur            time.Duration
}

// TimeDrift calculates some run time timing info
func (pb *PlayBack) TimeDrift() {
	maxDrift := 0.0
	var actSec runTimings
	for el := pb.timingsInfo.Front(); el != nil; el = el.Next() {
		actSec = el.Value.(runTimings)

		maxDrift = math.Max(maxDrift, math.Abs(actSec.driftDur.Seconds()/1000))
	}
	fmt.Printf("Max Drift between: %f(ms)\n", maxDrift)
	fmt.Printf("Expected Real run time %f(s)\n",
		(actSec.trdTime.Sub(pb.StartTime) / pb.simRatDur).Seconds())
	fmt.Println(pb.StartTime)
	fmt.Println(actSec.trdTime)
}
