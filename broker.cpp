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
#include <fcntl.h> 
#include "broker.hpp"

constexpr int PORT = 1235;
constexpr int BACKLOG = 5;
constexpr const char* ROOT_DIR = ".";

static bool is_running = true;

#define return_defer(val) do { result = (val); goto defer; } while (0)

void terminate(int) {
  fprintf(stderr, "INFO: received terminate signal exiting\n");
  is_running = false;
}

bool parse_request(int connfd, Broker::Request* request) {
  int request_type = -1;
  if (read(connfd, &request_type, sizeof(int)) < 0) {
    return true;
  }
  request->type = (Broker::RequestType)request_type;

  int request_length = -1;
  if (read(connfd, &request_length, sizeof(int)) < 0) {
    return true;
  }
  request->data.resize(request_length);

  if (read(connfd, request->data.data(), request_length) < 0) {
    return true;
  }

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

    switch (request.type)
    {
    case Broker::REQUEST_METADATA:
      break;
    
    case Broker::REQUEST_READ:
      break;

    case Broker::REQUEST_WRITE:
      break;

    default:
      fprintf(stderr, "ERROR: unknown request \n");
      break;
    }
    
defer:
  close(connfd);
}

int main(int, char**) {
  int result = EXIT_SUCCESS;
  int socket_flags;
  struct sockaddr_in sockaddr;
  int reuseopt = 1;

  // Handling Control-C / SigInt
  signal(SIGINT, terminate);

  // Open a Socket 
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    fprintf(stderr, "ERROR: cannot open socket\n");
    return_defer(EXIT_FAILURE);
  }

  // Setting non blocking mode
  socket_flags = fcntl(sockfd, F_GETFL, 0);
  if (fcntl(sockfd, F_SETFL, socket_flags | O_NONBLOCK) == -1) {
    fprintf(stderr, "ERROR: fcntl failed\n");
    return_defer(EXIT_FAILURE);
  }


  // Setting port reuse
  if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuseopt, sizeof(reuseopt)) < 0) {
    fprintf(stderr, "ERROR: setsockopt failed\n");
    return_defer(EXIT_FAILURE);
  }

  sockaddr.sin_family = AF_INET;
  sockaddr.sin_addr.s_addr = htonl(INADDR_ANY);
  sockaddr.sin_port = htons(PORT);

  if (bind(sockfd, (const struct sockaddr*)&sockaddr, sizeof(sockaddr)) < 0) {
    fprintf(stderr, "ERROR: cannot bind socket\n");
    return_defer(EXIT_FAILURE);
  }

  // Now listening for incoming connections
  if (listen(sockfd, BACKLOG) < 0) {
    fprintf(stderr, "ERROR: cannot listen to socket\n");
    return_defer(EXIT_FAILURE);
  }

  fprintf(stderr, "INFO: server is listening \n");

  while(is_running) {
    int connfd = accept(sockfd, nullptr, nullptr);
    if (connfd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        usleep(100000);
        continue;
      } else {
        fprintf(stderr, "ERROR: unable to accept connections\n");
        return_defer(EXIT_FAILURE);
      }
    }
   
    // TODO: Handle connection on seperate thread 
    handle_connection(connfd);
  }

  fprintf(stderr, "INFO: server shutdown gracefully\n");

defer:
  close(sockfd);
  return result;
}

