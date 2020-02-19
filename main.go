package main

import (
	"database/sql"
	"fmt"
	"log"
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

func lastDate(db *sql.DB) time.Time {
	var t time.Time
	sqlStatement := `SELECT time FROM eurusd ORDER BY time DESC LIMIT 1;`
	row := db.QueryRow(sqlStatement)
	switch err := row.Scan(&t); err {
	case sql.ErrNoRows:
		fmt.Println("No rows were returned!")
		return time.Now().Add(time.Hour * -1 * 24 * 365 * 20)
	case nil:
		return t
	default:
		panic(err)
	}
}

func updateDb(db *sql.DB) {

	for lastDate(db).Before(time.Now().Add(time.Minute)) {
		t := lastDate(db)
		fmt.Println(t)
		txn, err := db.Begin()
		if err != nil {
			panic(err)
		}

		stmt, err := txn.Prepare(pq.CopyIn("eurusd", "time", "open", "high", "low", "close", "volume"))
		if err != nil {
			panic(err)
		}
		minuteBars := getMinuteBars(t).Candles

		for _, bar := range minuteBars {
			if bar.Complete {
				//_, err := db.Query(`INSERT INTO eurusd (time, open, high, low, close, volume) VALUES ($1, $2, $3, $4, $5, $6);`, bar.Time, bar.Bid.O, bar.Bid.H, bar.Bid.L, bar.Bid.C, bar.Volume)
				_, err := stmt.Exec(bar.Time, bar.Bid.O, bar.Bid.H, bar.Bid.L, bar.Bid.C, bar.Volume)
				if err != nil {
					panic(err)
				}
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

func main() {
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

	fmt.Println("Successfully connected!")
	fmt.Println("Last time: $1", lastDate(db))
	updateDb(db)
}
