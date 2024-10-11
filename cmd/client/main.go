package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ayushkumar121/event-broker/pkg/client"
)

const (
	BROKER_ADDRESS = "localhost:8080"
)

func usageError(message string) {
	fmt.Fprintf(os.Stderr, "%v\n%v <flags> SUBCOMMAND TOPIC\n", message, os.Args[0])
	fmt.Fprintf(os.Stderr, "Subcommands:\n listen send\nFlags:\n")
	flag.PrintDefaults()
	os.Exit(1)
}

func main() {
	var partition int
	flag.IntVar(&partition, "p", 0, "partition for topic default is 0")

	var message string
	flag.StringVar(&message, "m", "", "message to be sent to broker")
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		usageError("invalid usage")
	}

	subcommand := args[0]
	topic := args[1]

	switch subcommand {
	case "listen":
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		consumerClient, err := client.NewConsumerClient([]string{BROKER_ADDRESS})
		if err != nil {
			panic(err)
		}
		defer consumerClient.Shutdown()

		consumerClient.AddConsumer(topic, uint32(partition), func(result client.ConsumerResult) {
			if result.Err() != nil {
				fmt.Println("Error received:", result.Err())
				return
			}

			fmt.Println("Received:", result.Response())
		})

		<-sigs
		fmt.Println("termination received")

	case "send":
		if message == "" {
			usageError("message cannot be empty")
		}

		producer, err := client.NewProducerClient([]string{BROKER_ADDRESS})
		if err != nil {
			panic(err)
		}

		offset, err := producer.SendMessage(topic, uint32(partition), []byte(message))
		if err != nil {
			panic(err)
		}

		fmt.Println("message written on offset : ", offset)
	default:
		usageError("unknown subcommand ")
	}
}
