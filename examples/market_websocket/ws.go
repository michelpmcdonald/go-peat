// Package main creates a simple playback webserver.
// After starting, navigate to http://localhost:8080/chart to view
// an ES futures playback from 09/03/2013.
package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/michelpmcdonald/go-peat"
	"github.com/michelpmcdonald/go-peat/examples/tsprovider"
)

type pbCmd struct {
	Cmd string
	Data string
}

func main() {
	http.HandleFunc("/ws", wsHandler)
	http.HandleFunc("/chart", chartHandler)

	panic(http.ListenAndServe(":8080", nil))
}

func chartHandler(w http.ResponseWriter, r *http.Request) {
	content, err := ioutil.ReadFile("./examples/market_websocket/chrt.html")
	if err != nil {
		fmt.Println("Could not open file.", err)
	}
	fmt.Fprintf(w, "%s", content)
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != "http://"+r.Host {
		//http.Error(w, "Origin not allowed", 403)
		fmt.Println("Cross Org")
		//return
	}
	conn, err := websocket.Upgrade(w, r, w.Header(), 1024, 1024)
	if err != nil {
		http.Error(w, "Could not open websocket connection", http.StatusBadRequest)
	}

	go sendSimTicks(conn)
}

func commandMonitor(conn *websocket.Conn, pb *gopeat.PlayBack) {
	// Read client commands
	for {
		m := pbCmd{}

		err := conn.ReadJSON(&m)
		if err != nil {
			fmt.Println("Error reading json.", err)
			break
		}

		fmt.Println(m)

		if m.Cmd == "play" {
			pb.Play()
		}
		if m.Cmd == "quit" {
			pb.Quit()
			break
		}
		if m.Cmd == "pause" {
			pb.Pause()
		}
		if m.Cmd == "resume" {
			pb.Resume()
		}

		if m.Cmd == "setRate" {
			rate, err := strconv.ParseUint(m.Data, 10, 16)
			if err != nil {
				fmt.Println("SetRate Cmd Error: ", err)
				continue
			}
			fmt.Println("Rate")
			pb.SetRate(uint16(rate))
		}
	}
}

func sendSimTicks(conn *websocket.Conn) {
	defer conn.Close()

	// Set up sim start and end times
	sym := "mes"
	simStart := time.Date(2013, 9, 3, 8, 30, 0, 0, time.UTC)
	simEnd := time.Date(2013, 9, 3, 10, 30, 0, 0, time.UTC)

	// Create a new timestamper data source
	fn := "./examples/tsprovider/ES_Trades.csv"
	csvFile, errFile := os.Open(fn)
	if errFile != nil {
		panic(errFile)
	}
	defer csvFile.Close()
	tsSource := &gopeat.CsvTsSource{
		Symbol:    sym,
		CsvStream: csvFile,
		CsvTsConv: tsprovider.TdiCsvToTrd,
	}
	tsSource.MaxRecs = 15000000

	// Create a new playback
	pb, errp := gopeat.New(
		sym,
		simStart,
		simEnd,
		tsSource,
		100,
		nil)
	if errp != nil {
		return
	}

	// Create a playback callback to send playback's
	// simulation time data output our websocket
	pb.SendTs = func(ts gopeat.TimeStamper) error {
		err := conn.WriteJSON(ts.(tsprovider.Trade))
		if err != nil {
			fmt.Println(err)
			pb.Quit()
		}
		return err
	}

	go commandMonitor(conn, pb)
	pb.Wait()
	fmt.Println("Playback concluded")
}
