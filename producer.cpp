#include <stdio.h>
#include <string>
#include <string_view>
#include <vector>
#include <stdlib.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "broker.hpp"

constexpr const char* BROKER_HOST = "127.0.0.1";
constexpr int PORT = 1235;

int main(int, char**) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    fprintf(stderr, "ERROR: cannot open socket\n");
    exit(EXIT_FAILURE);
  }

  struct sockaddr_in sockaddr;
  sockaddr.sin_family = AF_INET;
  sockaddr.sin_addr.s_addr = inet_addr(BROKER_HOST);
  sockaddr.sin_port = htons(PORT);

  if (connect(sockfd, (const struct sockaddr*)&sockaddr, sizeof(sockaddr)) < 0) {
    fprintf(stderr, "ERROR: cannot connect to socket\n");
    exit(EXIT_FAILURE);
  }

  std::vector<uint8_t> message = {0, 1, 0};
  if (Broker::write_message(sockfd, "", 0, message)) {
    fprintf(stderr, "ERROR: cannot write message\n");
    exit(EXIT_FAILURE);
  }

  close(sockfd);
  return 0;
}

