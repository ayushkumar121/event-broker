package client

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/ayushkumar121/event-broker/pkg/protocol"
)

const (
	RECONNECTION_DELAY   = time.Second * 30
	RECONNECTION_RETRIES = 10
	POLLING_DELAY        = time.Second
)

type ConsumerResult struct {
	err      error
	response *protocol.ReadResponse
}

func (r *ConsumerResult) Err() error {
	return r.err
}

func (r *ConsumerResult) Response() *protocol.ReadResponse {
	return r.response
}

type consumerHandlerFunc func(ConsumerResult)

type ConsumerClient struct {
	*brokerClient
	connections map[string]net.Conn
}

func NewConsumerClient(bootstrapBrokers []string) (*ConsumerClient, error) {
	brokerClient, err := newBrokerClient(bootstrapBrokers)
	if err != nil {
		return nil, err
	}

	return &ConsumerClient{
		brokerClient: brokerClient,
		connections:  make(map[string]net.Conn),
	}, nil
}

func (client *ConsumerClient) AddConsumer(topic string, partition uint32, handler consumerHandlerFunc) error {
	conn, err := client.connect(topic, partition)
	if err != nil {
		return err
	}
	go client.consumerHandler(topic, partition, conn, handler)
	return nil
}

func (client *ConsumerClient) Shutdown() {
	for _, conn := range client.connections {
		conn.Close()
	}
}

func (client *ConsumerClient) consumerHandler(topic string, partition uint32, conn net.Conn, handler consumerHandlerFunc) {
	var lastOffset protocol.Offset = 0

	for {
		req := &protocol.ReadRequest{
			LastOffset: lastOffset,
			Topic:      topic,
			Partition:  partition,
		}

		err := protocol.EncodeRequest(conn, req)
		if err != nil {
			log.Printf("cannot encode response %v\n", err)
			conn, err = client.reconnect(topic, partition)
			if err != nil {
				log.Printf("reconnection failed due to %v", err)
				return
			}
			continue
		}

		res, err := protocol.DecodeResponse(conn)
		if err != nil {
			log.Printf("cannot decode response %v\n", err)
			conn, err = client.reconnect(topic, partition)
			if err != nil {
				log.Printf("reconnection failed due to %v", err)
				return
			}
			continue
		}

		switch res.GetType() {
		case protocol.RESPONSE_READ:
			readResponse := res.(*protocol.ReadResponse)
			if readResponse.Offset == 0 {
				time.Sleep(time.Second)
				continue
			}
			lastOffset = readResponse.Offset

			handler(ConsumerResult{
				err:      nil,
				response: readResponse,
			})

		case protocol.RESPONSE_ERROR:
			handler(ConsumerResult{
				err:      errors.New(res.(*protocol.ErrorResponse).Message),
				response: nil,
			})

		default:
			panic("unknown response type")
		}
	}
}

func (client *ConsumerClient) connect(topic string, partition uint32) (net.Conn, error) {
	broker := client.getBroker(topic, partition)

	conn, err := net.Dial("tcp", broker)
	if err != nil {
		return nil, err
	}
	client.connections[consumerId(topic, partition)] = conn
	return conn, nil
}

func (client *ConsumerClient) reconnect(topic string, partition uint32) (net.Conn, error) {
	log.Printf("client disconnection attempting reconnection in %v\n", RECONNECTION_DELAY)

	for i := 0; i < RECONNECTION_RETRIES; i++ {
		time.Sleep(RECONNECTION_DELAY)
		conn, err := client.connect(topic, partition)
		if err != nil {
			log.Printf("reconnection failed due to %v", err)
			continue
		}
		return conn, nil
	}

	return nil, errors.New("reconnection retries exhausted")
}

func consumerId(topic string, partition uint32) string {
	return fmt.Sprintf("%v-%v", topic, partition)
}
