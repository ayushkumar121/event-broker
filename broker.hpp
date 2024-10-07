#pragma once

#include <string>
#include <stdint.h>

namespace Broker {

  enum RequestType {
    REQUEST_METADATA = 0x0,
    REQUEST_READ,
    REQUEST_WRITE
  };

  struct Request {
     RequestType type;
     int length;
     char* data;
  };

}
