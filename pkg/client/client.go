package client

import "errors"

type brokerClient struct {
	brokers []string
	// TODO: store metadata about topics and partitions
}

func newBrokerClient(bootstrapBrokers []string) (*brokerClient, error) {
	if len(bootstrapBrokers) == 0 {
		return nil, errors.New("bootstrap brokers cannot be empty")
	}

	// TODO: Request metadata and get all brokers and topics and partitions
	return &brokerClient{
		brokers: bootstrapBrokers,
	}, nil
}

func (c *brokerClient) getBroker(string, uint32) string {
	// TODO: Figure out the correct broker for topic and partition
	return c.brokers[0]
}
