#include "util.h"

#include <errno.h>

bool would_block(int rv) {
  return rv == -1 && (errno == EAGAIN || errno == EWOULDBLOCK);
}

size_t get_zero_lo_watermark(void *data) {
  return 0;
}

ssize_t zero_consume_signal(void *data) {
  return 0;
}
