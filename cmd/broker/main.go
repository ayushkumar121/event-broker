package main

import (
  "net"
  "log"
  "os"
  "os/signal"
  "syscall"
)

const (
  PORT = "8080"
)

func main() {
  sigs := make(chan os.Signal, 1)
  signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
  
  ln, err := net.Listen("tcp", ":"+PORT)
  if err != nil {
    log.Fatalf("cannot start server %v", err)
  }
  defer ln.Close()

  go func() {
    <-sigs
    log.Printf("termination signal received")
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
  conn.Close() 
}
