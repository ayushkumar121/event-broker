package client

import (
	"errors"
	"net"

	"github.com/ayushkumar121/event-broker/pkg/protocol"
)

type ProducerClient struct {
	*brokerClient
}

func NewProducerClient(bootstrapBrokers []string) (*ProducerClient, error) {
	brokerClient, err := newBrokerClient(bootstrapBrokers)
	if err != nil {
		return nil, err
	}

	return &ProducerClient{
		brokerClient,
	}, nil
}

func (client *ProducerClient) SendMessage(topic string, partition uint32, message []byte) (int64, error) {
	broker := client.getBroker(topic, partition)
	conn, err := net.Dial("tcp", broker)
	if err != nil {
		return -1, err
	}
	defer conn.Close()

	req := &protocol.WriteRequest{
		Topic:     topic,
		Partition: partition,
		Message:   message,
	}

	err = protocol.EncodeRequest(conn, req)
	if err != nil {
		return -1, err
	}

	res, err := protocol.DecodeResponse(conn)
	if err != nil {
		return -1, err
	}

	switch res.GetType() {
	case protocol.RESPONSE_WRITE:
		return res.(*protocol.WriteResponse).Offset, nil

	case protocol.RESPONSE_ERROR:
		return -1, errors.New(res.(*protocol.ErrorResponse).Message)

	default:
		panic("unknown response type")
	}
}
