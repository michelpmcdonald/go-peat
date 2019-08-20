package gopeat

import (
	"sync"
	"testing"
	"time"
)

type mockTsData struct {
	Tim time.Time
	Val int64
}

func (ts mockTsData) GetTimeStamp() time.Time {
	return ts.Tim
}

type mockTsDataSource struct {
	MaxRecs      int64
	RecCnt       int64
	DataInterval time.Duration
	StartTime    time.Time
}

func (st *mockTsDataSource) Next() (TimeStamper, bool) {
	for {
		st.RecCnt++
		if st.RecCnt > st.MaxRecs {
			return nil, false
		}
		trd := mockTsData{
			Tim: st.StartTime,
			Val: st.RecCnt,
		}
		st.StartTime.Add(st.DataInterval)
		return trd, true
	}
}

func (st *mockTsDataSource) SetStartTime(startTime time.Time) {
}

func (st *mockTsDataSource) SetEndTime(endTime time.Time) {
}

// A datasource that blocks on next, client should release Wg so next
// does not block the load data goroutine from shutting down
type mockTsBlockingDs struct {
	Wg         sync.WaitGroup
	NextCalled bool
}

func (st *mockTsBlockingDs) Next() (TimeStamper, bool) {
	st.Wg.Add(1)
	st.NextCalled = true
	st.Wg.Wait()
	return nil, false
}

func (st *mockTsBlockingDs) SetStartTime(startTime time.Time) {
}

func (st *mockTsBlockingDs) SetEndTime(endTime time.Time) {
}

// TestSendSpeed confirms that a value sent into playback is sent within
// 3 milliseconds of the proper time
func TestSendSpeed(t *testing.T) {
	// Create a new PlayBack at 2x rate
	var mts mockTsDataSource
	simStartTime := time.Now()
	dataTime := simStartTime.Add(time.Second * 1)
	pb, _ := New("test", simStartTime, dataTime, &mts, 2, nil)

	// Callback measures time to first data playback send.  Since
	// the first time stamper is 1 second after playback start,
	// the first callback should be at .5 seconds given the rate is 2x
	callbackHit := false
	pb.SendTs = func(ts TimeStamper) error {
		wallDur := time.Since(simStartTime)
		callbackHit = true
		if ts.(mockTsData).Val != 6 {
			t.Errorf("Val = %d; want 6", ts.(mockTsData).Val)
		}
		if wallDur.Seconds() >= .503 {
			t.Errorf("Time = %f; want less than .503", wallDur.Seconds())
		}
		return nil
	}
	pb.init()
	// Create a timestamper with a timestamp 1 second out after
	// start time and push it into the playback data chan
	pb.tsDataChan <- []TimeStamper{mockTsData{Tim: dataTime, Val: 6}}
	close(pb.tsDataChan)

	// run send, it will execute callback
	pb.wg.Add(1)
	pb.send()

	// Make sure call was called
	if !callbackHit {
		t.Errorf("Provided PlayBack call was not executed")
	}
}

// TestSimRate confirms that the user provided sim rate is translated
// into PlayBacks simRateDur properly.  A sim rate of 2x and a duration
// of 4mins should result in a sim duration of 2mins
func TestSimRate(t *testing.T) {
	var mts mockTsDataSource
	pb, _ := New("test", time.Now(), time.Now(), &mts, 2, nil)
	dur := time.Duration(time.Minute * 4)
	simTime := (dur / pb.simRatDur)
	if simTime.Minutes() != 2 {
		t.Errorf("dur(4) / 2 = %f; want 2", simTime.Minutes())
	}
}

func TestLoadTsData(t *testing.T) {
	// Create a new mocked data source that emits 23 time stamper values
	mts := mockTsDataSource{MaxRecs: 23}

	// Create a new playback that uses mts
	pb, _ := New("test", time.Now(), time.Now(), &mts, 2, nil)
	pb.init()

	// Set a read buffer size of 10
	pb.tsDataBufSize = 10

	// load data into the chan
	go pb.loadTimeStampedData()

	readCnt := 0
	bufLens := [6]int{10, 10, 3}
	// Read the data in the chan
	for dataBuf := range pb.tsDataChan {
		readCnt++
		if len(dataBuf) != bufLens[readCnt-1] {
			t.Errorf("buf %d length: %d; expected %d",
				readCnt,
				len(dataBuf),
				bufLens[readCnt-1])
		}
	}

	//Should have 3 reads from the chan
	if readCnt != 3 {
		t.Errorf("Got %d Chan reads; expected 3", readCnt)
	}
}

func TestCreateNoSource(t *testing.T) {
	_, err := New("test", time.Now(), time.Now(), nil, 2, nil)

	if err == nil {
		t.Errorf("Got Empty error, expected error")
	}
	expected := "playBack: tsSource required"
	if err.Error() != "playBack: tsSource required" {
		t.Errorf("Got %s error, expected %s", err.Error(), expected)
	}
}

func TestPlay(t *testing.T) {
	// Create a new mocked data source that emits 23 time stamper values
	mts := mockTsBlockingDs{}

	// Create a new playback that uses mts
	pb, _ := New("test", time.Now(), time.Now(), &mts, 2, nil)

	// Active flag should start as false
	if pb.replayActive {
		t.Errorf("replay active true, expected false")
	}

	pb.Play()

	// play worked if it set active flag to true
	if !pb.replayActive {
		t.Errorf("replay active false, expected true")
	}

	// play should start data loading from TimeStamper Source, so
	// confirm source's Next() was called
	if !mts.NextCalled {
		t.Error("NextCalled is false, expected true")
	}

	// Release mts so we don't leak loader goroutine
	close(pb.quitChan)
	mts.Wg.Done()

	// Ok at this point, no data was loaded, dataloader should be
	// closing, and sender should be closing and releasing PlayBacks
	// Blocking wait
	pb.Wait()

	// Make sure loader closed dat chan to loader confirm exit-cleanup
	select {
	case _, ok := <-pb.tsDataChan:
		// should be closed with no data
		if ok {
			t.Error("tsDataChan was not empty, expected it to be empty")
		}
	default:
		t.Error("tsDataChan is still open, expected it to be closed")
	}
}

func TestPause(t *testing.T) {
	// Create a new mocked data source that emits 23 time stamper values
	mts := mockTsBlockingDs{}

	// Create a new playback that uses mts
	pb, _ := New("test", time.Now(), time.Now(), &mts, 2, nil)

	// Need a pause chan for Pause() to signal
	pb.pauseChan = make(chan struct{})

	// Create a closed resumeChan, this is
	// a possible state after a pause-resume cycle
	pb.resumeChan = make(chan struct{})
	close(pb.resumeChan)
	pb.replayActive = true
	pb.paused = false

	pb.Pause()

	// Make sure pause closed pauseChan to signal a pause
	select {
	case _, ok := <-pb.pauseChan:
		// should be closed with no data
		if ok {
			t.Error("pauseChan was not empty, expected it to be empty")
		}
	default:
		t.Error("pauseChan is still open, expected it to be closed")
	}

	// Pause worked if it set active flag to true
	if !pb.paused {
		t.Error("pause flag is false, expected true")
	}

	select {
	case _, ok := <-pb.resumeChan:
		if !ok {
			t.Error("resumeChan is closed, expected it to be open")
		}
	default:
	}
}

func TestPauseAlreadyPaused(t *testing.T) {
	// Create a new mocked data source that emits 23 time stamper values
	mts := mockTsBlockingDs{}

	// Create a new playback that uses mts
	pb, _ := New("test", time.Now(), time.Now(), &mts, 2, nil)
	pb.pauseChan = make(chan struct{})
	pb.replayActive = false
	pb.paused = true

	pb.Pause()

	// Since pb is already in the paused state,
	// Pause() should do nothing
	select {
	case <-pb.pauseChan:
		t.Error("pauseChan signaled, expected to be un-signaled")
	default:
	}

	if pb.replayActive {
		t.Error("replay active true, expected to be false")
	}
	if !pb.paused {
		t.Error("paused false, expected to be true")
	}
}

func TestResume(t *testing.T) {
	// Create a new mocked data source that emits 23 time stamper values
	mts := mockTsBlockingDs{}

	// Create a new playback that uses mts
	pb, _ := New("test", time.Now(), time.Now(), &mts, 2, nil)

	// Need a resume chan for Resume() to signal
	pb.resumeChan = make(chan struct{})

	// Create a closed pauseChan, this is
	// a possible state after a pause
	pb.pauseChan = make(chan struct{})
	close(pb.pauseChan)
	pb.paused = true

	pb.Resume()

	// Make sure resume closed resumeChan to signal a pause
	select {
	case _, ok := <-pb.resumeChan:
		// should be closed with no data
		if ok {
			t.Error("resumeChan was not empty, expected it to be empty")
		}
	default:
		t.Error("resumeChan is still open, expected it to be closed")
	}

	// Pause worked if it set active flag to true
	if pb.paused {
		t.Error("pause flag is false, expected true")
	}

	select {
	case _, ok := <-pb.pauseChan:
		if !ok {
			t.Error("pausehan is closed, expected it to be open")
		}
	default:
	}
}

func TestQuit(t *testing.T) {
	// Create a new mocked data source that emits 23 time stamper values
	mts := mockTsBlockingDs{}

	// Create a new playback that uses mts
	pb, _ := New("test", time.Now(), time.Now(), &mts, 2, nil)
	pb.replayActive = true
	pb.quitChan = make(chan struct{})

	pb.Quit()

	select {
	case <-pb.quitChan:
	default:
		t.Error("quitChan not signaled, expected it to be signaled")
	}
}
