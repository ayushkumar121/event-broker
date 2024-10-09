package main

import (
	"log"
	"net"

	"github.com/ayushkumar121/event-broker/pkg/protocol"
)

const (
	BROKER_ADDRESS = "localhost:8080"
)

func main() {
	conn, err := net.Dial("tcp", BROKER_ADDRESS)
	if err != nil {
		log.Fatalf("cannot estalish connection %v", err)
	}
	defer conn.Close()

	req := &protocol.WriteRequest{
		Topic:     "test",
		Partition: 0,
		Message:   []byte("Hello World"),
	}

	err = protocol.EncodeRequest(conn, req)
	if err != nil {
		panic(err)
	}
}
