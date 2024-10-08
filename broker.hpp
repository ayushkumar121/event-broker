#pragma once

#include <string>
#include <string_view>
#include <vector>
#include <span>
#include <stdint.h>
#include <unistd.h>

namespace Broker {

  enum RequestType {
    REQUEST_METADATA = 0x0,
    REQUEST_READ,
    REQUEST_WRITE
  };

  struct Request {
    RequestType type;
    std::vector<uint8_t> data;
  };

  struct Metadata {
    int foo;
  };

  std::vector<uint8_t> serialise_request(RequestType type, std::span<uint8_t> bytes) {
    int length = bytes.size();
    std::vector<uint8_t> buffer;
    // Reserving space for request.type + request.length + Data
    buffer.reserve(8 + length);

    uint8_t* buf = (uint8_t*)&type;
    for (int i = 0; i < 4; i++) {
      buffer.push_back(buf[i]);
    }

    buf = (uint8_t*)&length;
    for (int i = 0; i < 4; i++) {
      buffer.push_back(buf[i]);
    }

    for (int i = 0; i < length; i++) {
      buffer.push_back(bytes[i]);
    }

    return buffer;
  }

  bool get_metadata(int sockfd, std::string_view, int, Metadata*) {
    auto request_buffer = serialise_request(REQUEST_METADATA, std::span<uint8_t>());
    if(write(sockfd, request_buffer.data(), request_buffer.size()) < 0) {
      return true;
    }
    return false;
  }

  bool write_message(int sockfd, std::string_view, int, std::span<uint8_t> message) {
    auto request_buffer = serialise_request(REQUEST_WRITE, message);
    if(write(sockfd, request_buffer.data(), request_buffer.size()) < 0) {
      return true;
    }
    return false;
  }
}
