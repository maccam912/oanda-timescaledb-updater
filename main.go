package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

const (
	host   = "localhost"
	port   = 5432
	user   = "postgres"
	dbname = "fx"
	url    = "https://api-fxtrade.oanda.com"
)

func lastDate(db *sql.DB, instrument, price string) time.Time {
	var t time.Time
	// tableName := fmt.Sprintf("%v_%v_s5", instrument, price)
	sqlStatement := fmt.Sprintf(`SELECT time FROM data WHERE pair = '%v' ORDER BY time DESC LIMIT 1;`, instrument)
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
	// for lastDate(db, instrument, price).Before(time.Now().Add(time.Minute)) {
	for lastDate(db, instrument, price).Before(time.Now().Add(24 * 4 * -1 * time.Hour)) {
		t := lastDate(db, instrument, price)
		txn, err := db.Begin()
		if err != nil {
			panic(err)
		}

		// tableName := fmt.Sprintf("%v_%v_s5", instrument, price)
		stmt, err := txn.Prepare(pq.CopyIn("data", "pair", "time", "open", "high", "low", "close", "volume"))
		if err != nil {
			panic(err)
		}
		<-timer
		minuteBars := getMinuteBars(client, t, instrument, price).Candles
		for _, bar := range minuteBars {
			if bar.Complete {
				_, err := stmt.Exec(instrument, bar.Time, bar.Price.O, bar.Price.H, bar.Price.L, bar.Price.C, bar.Volume)
				if err != nil {
					panic(err)
				}
			} else {
				time.Sleep(1 * time.Minute)
			}
		}

		err = stmt.Close()
		if err != nil {
			panic(err)
		}

		err = txn.Commit()
		if err != nil {
			panic(err)
		}
	}
}

func createTables(db *sql.DB, pairs, prices []string) {
	createFmtStr := `CREATE TABLE IF NOT EXISTS data (
		pair VARCHAR(7) NOT NULL,
		time	TIMESTAMPTZ	NOT NULL,
		open  DOUBLE PRECISION NULL,
		high  DOUBLE PRECISION NULL,
		low   DOUBLE PRECISION NULL,
		close DOUBLE PRECISION NULL,
		volume  INTEGER,
		CONSTRAINT "data_pk"
		PRIMARY KEY (pair, time)
	);
	  
	 CREATE INDEX IF NOT EXISTS pair_idx ON data (pair);
	 CREATE INDEX IF NOT EXISTS time_idx ON data (time);`
	// _, _ = db.Query("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;")
	// for _, pair := range pairs {
	// 	for _, price := range prices {
	// 		tableName := fmt.Sprintf("%v_%v_s5", pair, price)
	// 		stmt := fmt.Sprintf(createFmtStr, tableName)
	// 		_, err := db.Query(stmt)
	// 		if err != nil {
	// 			panic(err)
	// 		}
	// 		// _, err = db.Query(fmt.Sprintf(`SELECT create_hypertable('%v', 'time');`, tableName))
	// 		// if err != nil {
	// 		// 	log.Warn(err)
	// 		// }
	// 	}
	// }
	_, err := db.Query(createFmtStr)
	if err != nil {
		panic(err)
	}

}

func createViews(db *sql.DB, pairs, prices []string) {
	averagePrice := `CREATE OR REPLACE VIEW %v AS
	SELECT pair, time, (((2 * open) + (2 * close) + high + low)/6) as avg_price, volume
	FROM data WHERE pair = '%v';`

	baseView := `CREATE OR REPLACE VIEW %v AS
	SELECT * FROM data WHERE pair = '%v';`

	minView := `CREATE MATERIALIZED VIEW IF NOT EXISTS eurusd_1min AS
	SELECT pair, date_trunc('minute', min(time)) as time, avg(close) as close, sum(volume) as volume FROM data WHERE pair = 'eurusd' GROUP BY date_trunc('minute', time), pair;`

	hourView := `CREATE MATERIALIZED VIEW IF NOT EXISTS eurusd_1hour AS
	SELECT pair, date_trunc('hour', min(time)) as time, avg(close) as close, sum(volume) as volume FROM eurusd_1min WHERE pair = 'eurusd' GROUP BY date_trunc('hour', time), pair;`

	dayView := `CREATE MATERIALIZED VIEW IF NOT EXISTS eurusd_1day AS
	SELECT pair, date_trunc('day', min(time)) as time, avg(close) as close, sum(volume) as volume FROM eurusd_1hour WHERE pair = 'eurusd' GROUP BY date_trunc('day', time), pair;`

	weekView := `CREATE MATERIALIZED VIEW IF NOT EXISTS eurusd_1week AS
	SELECT pair, date_trunc('week', min(time)) as time, avg(close) as close, sum(volume) as volume FROM eurusd_1day WHERE pair = 'eurusd' GROUP BY date_trunc('week', time), pair;`

	monthView := `CREATE MATERIALIZED VIEW IF NOT EXISTS eurusd_1month AS
	SELECT pair, date_trunc('month', min(time)) as time, avg(close) as close, sum(volume) as volume FROM eurusd_1day WHERE pair = 'eurusd' GROUP BY date_trunc('month', time), pair;`

	yearView := `CREATE MATERIALIZED VIEW IF NOT EXISTS eurusd_1year AS
	SELECT pair, date_trunc('year', min(time)) as time, avg(close) as close, sum(volume) as volume FROM eurusd_1month WHERE pair = 'eurusd' GROUP BY date_trunc('year', time), pair;`

	// periodWeightedAverage := `CREATE OR REPLACE VIEW %v
	// AS
	// (SELECT time_bucket('%v', time) as time,
	// sum(avg_price*volume)/sum(volume) as avg_price,
	// sum(volume) as volume FROM %v
	// GROUP BY time_bucket('%v', time));`

	for _, stmt := range []string{minView, hourView, dayView, weekView, monthView, yearView} {
		_, err := db.Query(stmt)
		if err != nil {
			panic(err)
		}
	}

	for _, pair := range pairs {
		viewName := fmt.Sprintf("%v_avg_view", pair)
		stmt := fmt.Sprintf(averagePrice, viewName, pair)
		_, err := db.Query(stmt)
		if err != nil {
			panic(err)
		}
		viewName = fmt.Sprintf("%v_view", pair)
		stmt = fmt.Sprintf(baseView, viewName, pair)
		_, err = db.Query(stmt)
		if err != nil {
			panic(err)
		}
		// previousViewName := viewName
		// for _, period := range []string{"1m", "5m", "15m", "30m", "1h", "4h", "8h", "12h", "1d", "1w"} {
		// 	WAName := fmt.Sprintf(`%v_vol_weighted_%v_view`, pair, period)
		// 	stmt = fmt.Sprintf(periodWeightedAverage, WAName, period, previousViewName, period)
		// 	previousViewName = WAName
		// 	// _, err = db.Query(stmt)
		// 	if err != nil {
		// 		panic(err)
		// 	}
		// }
	}
}

func timergoroutine(c chan int) {
	for {
		c <- 1
		time.Sleep(time.Second / 100)
	}
}

func main() {
	dat, err := ioutil.ReadFile("pairs.csv")
	ps := string(dat)
	if err != nil {
		panic(err)
	}

	var modified []string
	for _, line := range strings.Split(ps, "\n") {
		modded := strings.ReplaceAll(line, "/", "")
		lowered := strings.ToLower(modded)
		if len(lowered) > 0 {
			modified = append(modified, strings.TrimSpace(lowered))
		}
	}
	pairs := modified
	prices := []string{"mid"}

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
	//createViews(db, pairs, prices)

	fmt.Println("Successfully connected!")

	timer := make(chan int, 2)
	var wg sync.WaitGroup

	go timergoroutine(timer)
	client := &http.Client{}
	for _, pair := range pairs {
		for _, price := range prices {
			fmt.Println(pair)
			go updateDb(client, db, pair, price, timer, &wg)
			wg.Add(1)
		}
	}
	wg.Wait()
}
