package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/ayushkumar121/event-broker/pkg/protocol"
)

const (
	PORT = "8080"
)

func main() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

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

func handleConnection(conn net.Conn) {
	defer conn.Close()

	request, err := protocol.DecodeRequest(conn)
	if err != nil {
		log.Printf("cannot parse request %v\n", err)
		return
	}

	log.Println(request)
}
