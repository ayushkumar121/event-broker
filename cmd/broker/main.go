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

	req, err := protocol.DecodeRequest(conn)
	if err != nil {
		log.Printf("cannot parse request %v\n", err)
		return
	}

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
		log.Printf("cannot handle request %v\n", err)
		return
	}

	err = protocol.EncodeResponse(conn, res)
	if err != nil {
		log.Printf("cannot send response %v\n", err)
		return
	}
}

func handleMetadataReq(req *protocol.MetaDataRequest) (*protocol.MetaDataResponse, error) {
	return nil, nil
}

func handleReadReq(req *protocol.ReadRequest) (*protocol.ReadResponse, error) {
	return nil, nil
}

func handleWriteReq(req *protocol.WriteRequest) (*protocol.WriteResponse, error) {
	return nil, nil
}
