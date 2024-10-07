/*
 * Broker listens to producers and writes messages to the disk
 * */
#include <stdio.h>
#include <string>
#include <string_view>
#include <stdlib.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "broker.hpp"

constexpr int PORT = 8080;
constexpr int BACKLOG = 5;
constexpr const char* ROOT_DIR = ".";

static bool is_running = true;

void panic(const char* message) {
  fprintf(stderr, "PANICED: %s\n", message);
  exit(1);
}

void terminate(int) {
  fprintf(stderr, "INFO: received terminate signal exiting\n");
  is_running = false;
}

bool parse_request(int connfd, Broker::Request* request) {
  request->type = Broker::REQUEST_WRITE;
  return false;
}

bool write_message(std::string_view topic_name) {
  fprintf(stderr, "INFO: writing message to disk\n");
  return false;
}

void handle_connection(int connfd) {
    Broker::Request request;
    if (parse_request(connfd, &request)) {
      fprintf(stderr, "ERROR: cannot parse request\n");
      goto defer;
    }

    fprintf(stderr, "INFO: message received: %d\n", request.type);
defer:
  close(connfd);
}

int main(int, char**) {
  // Handling Control-C / SigInt
  signal(SIGINT, terminate);

  // Open a Socket 
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    panic("cannot open socket");
  }

  struct sockaddr_in sockaddr;
  sockaddr.sin_family = AF_INET;
  sockaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  sockaddr.sin_port = htons(PORT);

  if (bind(sockfd, (const struct sockaddr*)&sockaddr, sizeof(sockaddr)) < 0) {
    panic("cannot bind socket");
  }

  // Now listening for incoming connections
  if (listen(sockfd, BACKLOG) < 0) {
    panic("cannot listen to socket");
  }

  // Setting timeout for accept so it returns periodically
  struct timeval timeout;
  timeout.tv_sec = 1;
  timeout.tv_usec = 0;
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

  fprintf(stderr, "INFO: server is listening \n");

  while(is_running) {
    int connfd = accept(sockfd, nullptr, nullptr);
    if (connfd < 0) {
      continue;
    }
    
    handle_connection(connfd);
  }

  close(sockfd);
  fprintf(stderr, "INFO: server shutdown gracefully\n");
  return 0;
}

