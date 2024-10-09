package main

import (
	"database/sql"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "embed"

	"github.com/ayushkumar121/event-broker/pkg/protocol"
	_ "github.com/mattn/go-sqlite3"
)

const (
	PORT     = "8080"
	DATABASE = "broker.db"
)

var db *sql.DB

//go:embed migrations.sql
var migrations string

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	err := initDb()
	if err != nil {
		log.Fatalf("cannot connect to database %v\n", err)
	}
	defer db.Close()

	err = runMigrations()
	if err != nil {
		log.Fatalf("cannot run migrations %v\n", err)
	}

	ln, err := net.Listen("tcp", ":"+PORT)
	if err != nil {
		log.Fatalf("cannot start server %v\n", err)
	}
	defer ln.Close()

	go func() {
		<-sigs
		log.Println("termination signal received")
		ln.Close()
	}()

	log.Println("server started")

	for {
		conn, err := ln.Accept()
		if err != nil {
			break
		}

		go handleConnection(conn)
	}

	log.Println("server exiting")
}

// Connection handling

func handleConnection(conn net.Conn) {
	defer conn.Close()

	req, err := protocol.DecodeRequest(conn)
	if err != nil {
		log.Printf("cannot parse request %v\n", err)
		return
	}

	log.Printf("received request for %v\n", req.GetType())

	var res protocol.Response

	switch req.GetType() {
	case protocol.REQUEST_METADATA:
		res, err = handleMetadataReq(req.(*protocol.MetaDataRequest))

	case protocol.REQUEST_READ:
		res, err = handleReadReq(req.(*protocol.ReadRequest))

	case protocol.REQUEST_WRITE:
		res, err = handleWriteReq(req.(*protocol.WriteRequest))

	default:
		panic("unreachable")
	}

	if err != nil {
		protocol.EncodeResponse(conn, &protocol.ErrorResponse{
			Message: err.Error(),
		})
		return
	}

	err = protocol.EncodeResponse(conn, res)
	if err != nil {
		log.Printf("cannot send response %v\n", err)
		return
	}
}

func handleMetadataReq(req *protocol.MetaDataRequest) (*protocol.MetaDataResponse, error) {
	log.Println("Metadata...", req)
	return &protocol.MetaDataResponse{}, nil
}

func handleReadReq(req *protocol.ReadRequest) (*protocol.ReadResponse, error) {
	log.Println("Reading...", req)
	return &protocol.ReadResponse{
		Offset:  0,
		Message: []byte{},
	}, nil
}

func handleWriteReq(req *protocol.WriteRequest) (*protocol.WriteResponse, error) {
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var exists bool

	// Check if topic exists
	err = db.QueryRow("SELECT EXISTS(SELECT * from topics WHERE name=?)", req.Topic).Scan(&exists)
	if err != nil {
		return nil, err
	}

	// Check if partition exists
	err = db.QueryRow("SELECT EXISTS(SELECT * from partitions WHERE topic=? and partition=?)", req.Topic, req.Partition).Scan(&exists)
	if err != nil {
		return nil, err
	}

	// Write message into database
	timestamp := time.Now().Format(time.RFC3339)
	result, err := db.Exec("INSERT into messages(topic, partition, message, timestamp) values(?, ?, ?, ?)",
		req.Topic, req.Partition, req.Message, timestamp)
	if err != nil {
		return nil, err
	}

	offset, err := result.LastInsertId()
	if err != nil {
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return &protocol.WriteResponse{
		Offset: offset,
	}, nil
}

// Database

func initDb() error {
	var err error
	db, err = sql.Open("sqlite3", DATABASE)
	if err != nil {
		return err
	}

	err = db.Ping()
	if err != nil {
		return err
	}

	return nil
}

func runMigrations() error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Exec(migrations)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
