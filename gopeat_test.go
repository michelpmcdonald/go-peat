package gopeat

import (
	"fmt"
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

// TestPlay confirms that a value sent into playback is sent within
// 3 milliseconds of the proper time
func TestPlay(t *testing.T) {
	var mts mockTsDataSource
	var wg sync.WaitGroup
	wg.Add(1)
	simStartTime := time.Now() //11:00:00
	dataTime := simStartTime.Add(time.Second * 1)
	pb, _ := New("test", simStartTime, dataTime, &mts, 2, nil)
	pb.SendTs = func(ts TimeStamper) error {
		wallDur := time.Since(simStartTime)
		if ts.(mockTsData).Val != 6 {
			t.Errorf("Val = %d; want 6", ts.(mockTsData).Val)
		}
		if wallDur.Seconds() > .503 {
			t.Errorf("Time = %f; want less than .505", wallDur.Seconds())
		}
		return nil
	}
	// Write the time stampted data buffer to the data chan
	pb.tsDataChan <- []TimeStamper{mockTsData{Tim: dataTime, Val: 6}}
	close(pb.tsDataChan)
	pb.send(&wg)
	fmt.Println("done")
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

	// Set a read buffer size of 10
	pb.tsDataBufSize = 10

	// load data into the chan
	go pb.loadTimeStamptedData()

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
