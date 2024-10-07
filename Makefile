CXX=c++
CXXFLAGS=-Wall -Wpedantic -Wextra

broker: broker.cpp broker.hpp
	$(CXX) -o broker broker.cpp $(CXXFLAGS)
