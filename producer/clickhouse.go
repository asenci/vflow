//: ----------------------------------------------------------------------------
//: Copyright (C) 2017 Verizon.  All Rights Reserved.
//: All Rights Reserved
//:
//: file:    clickHouse.go
//: details: vflow tcp/udp producer plugin
//: author:  Joe Percivall
//: date:    12/18/2017
//:
//: Licensed under the Apache License, Version 2.0 (the "License");
//: you may not use this file except in compliance with the License.
//: You may obtain a copy of the License at
//:
//:     http://www.apache.org/licenses/LICENSE-2.0
//:
//: Unless required by applicable law or agreed to in writing, software
//: distributed under the License is distributed on an "AS IS" BASIS,
//: WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//: See the License for the specific language governing permissions and
//: limitations under the License.
//: ----------------------------------------------------------------------------

package producer

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"runtime"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/kshvakov/clickhouse"
	"gopkg.in/yaml.v2"
)

// ClickHouse represents ClickHouse producer
type ClickHouse struct {
	config  ClickHouseConfig
	counter *uint64
	logger  *log.Logger
	msgChan chan []byte
	sqlChan chan SQLRow
}

// ClickHouseConfig is the struct that holds all configuration for ClickHouseConfig connections
type ClickHouseConfig struct {
	Driver     string `yaml:"driver"`
	Table      string `yaml:"table"`
	URL        string `yaml:"url"`
	MsgWorkers int    `yaml:"msg-workers"`
	SqlRows    int    `yaml:"sql-rows"`
	SqlWorkers int    `yaml:"sql-workers"`
}

type DecodedField struct {
	ID    uint16      `json:"I"`
	Value interface{} `json:"V"`
}

type Message struct {
	AgentID  string
	DataSets [][]DecodedField
}

type SQLRow struct {
	host                 string
	sourceAddress        string
	sourceAs             uint32
	sourceInterface      string
	sourcePort           uint16
	destinationAddress   string
	destinationAs        uint32
	destinationInterface string
	destinationPort      uint16
	transportProtocol    uint8
	packets              uint64
	bytes                uint64
}

func (ch *ClickHouse) setup(configFile string, logger *log.Logger) error {
	var counter uint64

	ch.config = ClickHouseConfig{
		Driver:     "clickhouse",
		Table:      "netflow",
		URL:        "tcp://127.0.0.1:9000?debug=false",
		MsgWorkers: runtime.NumCPU(),
		SqlRows:    1000,
		SqlWorkers: 4,
	}

	if err := ch.load(configFile); err != nil {
		logger.Println(err)
		return err
	}

	ch.counter = &counter
	ch.logger = logger
	ch.msgChan = make(chan []byte, 1000)
	ch.sqlChan = make(chan SQLRow, 1000)

	go func() {
		var (
			cCount uint64
			pCount uint64
		)

		tik := time.Tick(time.Second)

		for range tik {
			cCount = atomic.LoadUint64(ch.counter)
			log.Printf("received %dflows/s\n", cCount - pCount)
			pCount = cCount
		}
	}()

	logger.Println("create database table")
	err := ch.SqlCreateTable()
	if err != nil {
		logger.Fatal(err)
	}

	logger.Printf("start %d SQL workers", ch.config.SqlWorkers)
	for i := 0; i < ch.config.SqlWorkers; i++ {
		go ch.SqlWorker(i)
	}

	logger.Printf("start %d message workers", ch.config.MsgWorkers)
	for i := 0; i < ch.config.MsgWorkers; i++ {
		go ch.MsgWorker(i)
	}

	return nil
}

func (ch *ClickHouse) inputMsg(topic string, mCh chan []byte, ec *uint64) {
	ch.logger.Println("start producer: ClickHouse")

	for {

		data, ok := <-mCh
		if !ok {
			ch.logger.Println("stop producer: ClickHouse")
			break
		}

		ch.msgChan <- data
	}
}

func (ch *ClickHouse) load(f string) error {
	b, err := ioutil.ReadFile(f)
	if err != nil {
		return err
	}

	err = yaml.Unmarshal(b, &ch.config)
	if err != nil {
		return err
	}

	return nil
}

func (ch *ClickHouse) MsgWorker(i int) {
	var (
		message Message
		row     SQLRow
	)

	for {
		data, ok := <-ch.msgChan
		if !ok {
			return
		}

		if err := json.Unmarshal(data, &message); err != nil {
			ch.logger.Printf("Message worker %d: %s\n", i, err)
		}

		for _, dataset := range message.DataSets {
			for _, field := range dataset {
				switch field.ID {
				case 1:
					switch v := field.Value.(type){
					case string:
						row.bytes, _ = strconv.ParseUint(v, 0, 64)
					case float64:
						row.bytes = uint64(v)
					}
				case 2:
					switch v := field.Value.(type){
					case string:
						row.packets, _ = strconv.ParseUint(v, 0, 64)
					case float64:
						row.packets = uint64(v)
					}
				case 4:
					row.transportProtocol = uint8(field.Value.(float64))
				case 7:
					row.sourcePort = uint16(field.Value.(float64))
				case 8, 27:
					row.sourceAddress = field.Value.(string)
				case 10:
					row.sourceInterface = strconv.Itoa(int(field.Value.(float64)))
				case 11:
					row.destinationPort = uint16(field.Value.(float64))
				case 12, 28:
					row.destinationAddress = field.Value.(string)
				case 14:
					row.destinationInterface = strconv.Itoa(int(field.Value.(float64)))
				case 16:
					switch v := field.Value.(type){
					case string:
						n, _ := strconv.ParseUint(v, 0, 64)
						row.sourceAs = uint32(n)
					case float64:
						row.sourceAs = uint32(v)
					}
				case 17:
					switch v := field.Value.(type){
					case string:
						n, _ := strconv.ParseUint(v, 0, 64)
						row.destinationAs = uint32(n)
					case float64:
						row.destinationAs = uint32(v)
					}
				}

			}
			row.host = message.AgentID
			ch.sqlChan <- row
		}

		atomic.AddUint64(ch.counter, uint64(len(message.DataSets)))
	}
}

func (ch *ClickHouse) SqlWorker(i int) {
	ch.logger.Printf("SQL worker %d: connect to database\n", i)
	db, err := sql.Open(ch.config.Driver, ch.config.URL)
	if err != nil {
		ch.logger.Fatal(err)
	}

	for {
		err := db.Ping()
		if err != nil {
			if exception, ok := err.(*clickhouse.Exception); ok {
				ch.logger.Printf("SQL worker %d: [%d] %s \n%s\n", i, exception.Code, exception.Message, exception.StackTrace)
			} else {
				ch.logger.Printf("SQL worker %d: %s\n", i, err)
			}
            time.Sleep(time.Second)
			continue
		}

		tx, err := db.Begin()
		if err != nil {
			ch.logger.Printf("SQL worker %d: %s\n", i, err)
            time.Sleep(time.Second)
			continue
		}

		stmt, err := tx.Prepare(fmt.Sprintf(`
			INSERT INTO %s (
				host,
				source_address,
				source_as,
				source_interface,
				source_port,
				destination_address,
				destination_as,
				destination_interface,
				destination_port,
				transport_protocol,
				packets,
				bytes
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, ch.config.Table))
		if err != nil {
			ch.logger.Printf("SQL worker %d: %s\n", i, err)
            time.Sleep(time.Second)
			continue
		}

		for i := 0; i < ch.config.SqlRows; i++ {
			row, ok := <-ch.sqlChan
			if !ok {
				return
			}
			_, err := stmt.Exec(
				row.host,
				row.sourceAddress,
				row.sourceAs,
				row.sourceInterface,
				row.sourcePort,
				row.destinationAddress,
				row.destinationAs,
				row.destinationInterface,
				row.destinationPort,
				row.transportProtocol,
				row.packets,
				row.bytes,
			)
			if err != nil {
				ch.logger.Printf("SQL worker %d: %s\n", i, err)
				continue
			}

		}

		err = tx.Commit()
		if err != nil {
			ch.logger.Printf("SQL worker %d: %s\n", i, err)
			continue
		}
	}
}

func (ch *ClickHouse) SqlCreateTable() error {
	db, err := sql.Open(ch.config.Driver, ch.config.URL)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			date Date DEFAULT toDate(timestamp),
			timestamp DateTime DEFAULT now(),
			host String,
			source_address String,
			source_as UInt32,
			source_interface String,
			source_port UInt16,
			destination_address String,
			destination_as UInt32,
			destination_interface String,
			destination_port UInt16,
			transport_protocol UInt8,
			packets UInt64,
			bytes UInt64
		) ENGINE = MergeTree(date, (source_address, destination_address, source_port, destination_port, transport_protocol), 8192)
	`, ch.config.Table))
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}
