package client

import (
	"errors"
	"net"
	"time"

	"github.com/ayushkumar121/event-broker/pkg/protocol"
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

type consumer struct {
	conn net.Conn
}

type ConsumerClient struct {
	*brokerClient
	consumers []consumer
}

func NewConsumerClient(bootstrapBrokers []string) (*ConsumerClient, error) {
	brokerClient, err := newBrokerClient(bootstrapBrokers)
	if err != nil {
		return nil, err
	}

	return &ConsumerClient{
		brokerClient: brokerClient,
		consumers:    make([]consumer, 0),
	}, nil
}

func (client *ConsumerClient) AddConsumer(topic string, partition uint32, handler consumerHandlerFunc) error {
	broker := client.getBroker(topic, partition)

	// TODO: Periodic re connections
	conn, err := net.Dial("tcp", broker)
	if err != nil {
		return err
	}
	client.consumers = append(client.consumers, consumer{conn})

	go consumerHandler(topic, partition, conn, handler)
	return nil
}

func (client *ConsumerClient) Shutdown() {
	for _, consumer := range client.consumers {
		consumer.conn.Close()
	}
}

// TODO: reconnection incase broker goes down
func consumerHandler(topic string, partition uint32, conn net.Conn, handler consumerHandlerFunc) {
	var lastOffset protocol.Offset = 0

	for {
		req := &protocol.ReadRequest{
			LastOffset: lastOffset,
			Topic:      topic,
			Partition:  partition,
		}

		err := protocol.EncodeRequest(conn, req)
		if err != nil {
			// TODO: connection disconnected here
			return
		}

		res, err := protocol.DecodeResponse(conn)
		if err != nil {
			return
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
