#include <stdio.h>
#include <string>
#include <string_view>
#include <stdlib.h>
#include <signal.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

#include "broker.hpp"

constexpr const char* BROKER_HOST = "127.0.0.1";
constexpr int PORT = 8080;

int main(int, char**) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0) {
    panic("cannot open socket");
  }

  struct sockaddr_in sockaddr;
  sockaddr.sin_family = AF_INET;
  sockaddr.sin_addr.s_addr = inet_addr(BROKER_HOST);
  sockaddr.sin_port = htons(PORT);

  if (connect(sockfd, (const struct sockaddr*)&sockaddr, sizeof(sockaddr)) < 0) {
    panic("cannot connect to socket");
  }

  close(sockfd);
  return 0;
}

