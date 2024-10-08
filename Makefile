CXX=c++
CXXFLAGS=-std=c++20 -Wall -Wpedantic -Wextra -g

all: producer broker

producer: producer.cpp broker.hpp
	$(CXX) -o producer producer.cpp $(CXXFLAGS)

broker: broker.cpp broker.hpp
	$(CXX) -o broker broker.cpp $(CXXFLAGS)
