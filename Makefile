all: client broker

client: prebuild
	go build -o bin/client ./cmd/client

broker: prebuild
	go build -o bin/broker ./cmd/broker

prebuild:
	mkdir -p bin
