package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

const (
	host   = "localhost"
	port   = 5432
	user   = "maccam912"
	dbname = "fx"
	url    = "https://api-fxtrade.oanda.com"
)

func lastDate(db *sql.DB, instrument, price string) time.Time {
	var t time.Time
	tableName := fmt.Sprintf("%v_%v_s5", instrument, price)
	sqlStatement := fmt.Sprintf(`SELECT time FROM %v ORDER BY time DESC LIMIT 1;`, tableName)
	row := db.QueryRow(sqlStatement)
	switch err := row.Scan(&t); err {
	case sql.ErrNoRows:
		result, err := time.Parse("2006-01-02 15:04:05", "2004-12-31 23:59:55")
		if err != nil {
			panic(err)
		}
		return result
	case nil:
		return t
	default:
		panic(err)
	}
}

func updateDb(client *http.Client, db *sql.DB, instrument, price string, timer chan int, wg *sync.WaitGroup) {
	defer wg.Done()
	for lastDate(db, instrument, price).Before(time.Now().Add(time.Minute)) {
		t := lastDate(db, instrument, price)
		txn, err := db.Begin()
		if err != nil {
			panic(err)
		}

		tableName := fmt.Sprintf("%v_%v_s5", instrument, price)
		stmt, err := txn.Prepare(pq.CopyIn(tableName, "time", "open", "high", "low", "close", "volume"))
		if err != nil {
			panic(err)
		}
		<-timer
		minuteBars := getMinuteBars(client, t, instrument, price).Candles

		for _, bar := range minuteBars {
			if bar.Complete {
				_, err := stmt.Exec(bar.Time, bar.Bid.O, bar.Bid.H, bar.Bid.L, bar.Bid.C, bar.Volume)
				if err != nil {
					panic(err)
				}
			} else {
				time.Sleep(1 * time.Minute)
			}
		}
		_, err = stmt.Exec()
		if err != nil {
			panic(err)
		}

		err = stmt.Close()
		if err != nil {
			log.Fatal(err)
		}

		err = txn.Commit()
		if err != nil {
			log.Fatal(err)
		}
	}
}

func createTables(db *sql.DB, pairs, prices []string) {
	createFmtStr := `CREATE TABLE IF NOT EXISTS %v (
		time	TIMESTAMPTZ	NOT NULL UNIQUE,
		open  NUMERIC NULL,
		high  NUMERIC NULL,
		low   NUMERIC NULL,
		close NUMERIC NULL,
		volume  NUMERIC NULL
	  );`
	for _, pair := range pairs {
		for _, price := range prices {
			tableName := fmt.Sprintf("%v_%v_s5", pair, price)
			stmt := fmt.Sprintf(createFmtStr, tableName)
			_, err := db.Query(stmt)
			if err != nil {
				panic(err)
			}
		}
	}
	_, _ = db.Query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
}

func timergoroutine(c chan int) {
	for {
		c <- 1
		time.Sleep(time.Second / 100)
	}
}

func main() {
	pairs := []string{"eurusd", "usdjpy", "gbpusd", "audusd", "usdchf", "nzdusd", "usdcad"}
	prices := []string{"bid", "ask"}

	psqlInfo := fmt.Sprintf("host=%s port=%d user=%s "+
		"dbname=%s sslmode=disable",
		host, port, user, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	err = db.Ping()
	if err != nil {
		panic(err)
	}

	createTables(db, pairs, prices)

	fmt.Println("Successfully connected!")

	timer := make(chan int, 2)
	var wg sync.WaitGroup

	go timergoroutine(timer)
	client := &http.Client{}
	for _, pair := range pairs {
		for _, price := range prices {
			go updateDb(client, db, pair, price, timer, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
}
