all: producer broker

producer: prebuild
	go build -o bin/producer ./cmd/producer

broker: prebuild
	go build -o bin/broker ./cmd/broker

prebuild:
	mkdir -p bin
