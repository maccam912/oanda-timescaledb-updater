package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"
)

const (
	ctLayout = "2006-01-02T15:04:05.000000000Z"
)

// OandaTime for unmarshaling
type OandaTime struct {
	time.Time
}

// BidPrices type
type BidPrices struct {
	O float64 `json:"o,string"`
	H float64 `json:"h,string"`
	L float64 `json:"l,string"`
	C float64 `json:"c,string"`
}

// Bar is a single candlestick bar
type Bar struct {
	Time     string    `json:"time"`
	Complete bool      `json:"complete"`
	Volume   float64   `json:"volume"`
	Bid      BidPrices `json:"bid"`
}

// InstrumentResponse mirrors the response from Oanda
type InstrumentResponse struct {
	Instrument  string `json:"instrument"`
	Granularity string `json:"granularity"`
	Candles     []Bar  `json:"candles"`
}

// UnmarshalJSON for unmarshaling a bar
func (ot *OandaTime) UnmarshalJSON(b []byte) (err error) {
	s := strings.Trim(string(b), "\"")
	if s == "null" {
		ot.Time = time.Time{}
		return
	}
	ot.Time, err = time.Parse(ctLayout, s)
	return
}

func getMinuteBars(client *http.Client, earliestTime time.Time, instrument, price string) InstrumentResponse {
	oandaInstrumentName := strings.ToUpper(fmt.Sprintf("%v_%v", instrument[0:3], instrument[3:6]))
	oandaPriceName := strings.ToUpper(price[0:1])
	fmt.Printf("Getting oanda data for %v of %v for %v\n", price, instrument, earliestTime)
	fullURL := url + fmt.Sprintf("/v3/instruments/%v/candles?price=%v&count=5000&granularity=S5&from=%v&includeFirst=false", oandaInstrumentName, oandaPriceName, earliestTime.Unix())
	req, err := http.NewRequest("GET", fullURL, nil)
	if err != nil {
		panic(err)
	}

	req.Header.Add("Authorization", "Bearer "+os.Getenv("OANDA_TOKEN"))
	response, err := client.Do(req)
	if err != nil {
		panic(err)
	}

	b, err := ioutil.ReadAll(response.Body)
	if err != nil {
		panic(err)
	}

	bars := InstrumentResponse{}
	err = json.Unmarshal(b, &bars)
	if err != nil {
		panic(err)
	}
	return bars
}
