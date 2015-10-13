#include "macro.h"
#include "socket.h"
#include "struct.h"

#include <assert.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

struct data {
  int sock;
  enum { R, S } mode;
  char spec[];
};

static bool init(void *data) {
  GET(struct data, this, data);
  CHECK_SYSCALL_OR_RETURN(
      false, this->sock = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0),
      "failed to create socket for ", this->spec);

  // TODO: check socket options
  return true;
}

static void destroy(void *data) {
  GET(struct data, this, data);
  COND_CHECK_SYSCALL_OR_WARN(this->sock, -1, close(this->sock),
                             "failed to close socket for ", this->spec);
  free(data);
}

static uint32_t get_epoll_event(void *data) {
  GET(struct data, this, data);
  return (this->mode == R) ? EPOLLIN : EPOLLOUT;
}

static int get_fd(void *data) {
  GET(struct data, this, data);
  return this->sock;
}

static const struct producer_ops recv_ops = {
  .init             = init,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
};

static const struct consumer_ops send_ops = {
  .init             = init,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
};

static struct data *construct(const char *spec, int mode) {
  struct data *data = malloc(
      sizeof(struct data) + strlen(spec) + 1);

  if (data) {
    data->sock = -1;
    data->mode = mode;
    strcpy(data->spec, spec);
  }

  return data;
}

struct producer get_socket_reader(const char *spec) {
  return (struct producer) {&recv_ops, construct(spec, R)};
}

struct consumer get_socket_writer(const char *spec) {
  return (struct consumer) {&send_ops, construct(spec, S)};
}
