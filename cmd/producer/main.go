package main

import (
  "net"
  "log"
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
  
  
}
