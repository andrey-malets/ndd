#include "socket.h"
#include "struct.h"

#include <stdlib.h>

struct producer get_socket_reader(const char *spec) {
  return (struct producer) {0, 0};
}

struct consumer get_socket_writer(const char *spec) {
  return (struct consumer) {0, 0};
}
