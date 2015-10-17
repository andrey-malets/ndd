#include "macro.h"
#include "socket.h"
#include "struct.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define DEFAULT_PORT "3634"
#define PORT_MAX_CHARS 5

static const unsigned int CONNECT_BACKOFF[] = {0, 1, 3, 5};

struct data {
  int sock;
  int client_sock;

  enum { R, S } mode;
  char port[PORT_MAX_CHARS+1];
  char host[];
};

static bool refused(int rv) {
  return rv == -1 && errno == ECONNREFUSED;
}

static int try_connect(int socket, struct addrinfo *ai, const char *host) {
  int rv = -1;
  for (size_t i = 0; i != arraysize(CONNECT_BACKOFF); ++i) {
    // ignore signals
    sleep(CONNECT_BACKOFF[i]);

    rv = connect(socket, ai->ai_addr, ai->ai_addrlen);

    if (refused(rv))
      continue;
    CHECK_SYSCALL_OR_WARN(
        rv, "warning: connect() failed for one of addresses for ", host);
    return rv;
  }

  CHECK_SYSCALL_OR_WARN(
      rv, "warning: connect() failed for one of addresses for ", host);
  return rv;
}

static struct addrinfo get_hints(int mode) {
  struct addrinfo rv;
  memset(&rv, 0, sizeof(struct addrinfo));
  rv.ai_family = AF_UNSPEC;
  rv.ai_socktype = SOCK_STREAM;
  rv.ai_protocol = 0;

  switch (mode) {
  case R:
    rv.ai_flags = 0;
    break;
  case S:
    rv.ai_flags = AI_PASSIVE;
    rv.ai_canonname = NULL;
    rv.ai_addr = NULL;
    rv.ai_next = NULL;
    break;
  default:
    assert(0);
  }

  return rv;
}

static bool init(void *data, size_t block_size) {
  GET(struct data, this, data);

  bool retval = true;

  struct addrinfo hints = get_hints(this->mode);
  struct addrinfo *result;

  // TODO: check this shit error codes
  CHECK_OR_RETURN(
      false, getaddrinfo(strlen(this->host) ? this->host : NULL, this->port,
                         &hints, &result) == 0,
      "getaddrinfo() failed");

  for (struct addrinfo *i = result; i != NULL; i = i->ai_next) {
    CHECK_SYSCALL_OR_WARN(
        this->sock = socket(i->ai_family, i->ai_socktype, i->ai_protocol),
        "warning: socket() failed for one of addresses for ", this->host);

    if (this->sock == -1)
      continue;

    int rv = -1;
    switch (this->mode) {
      case R:
        rv = try_connect(this->sock, i, this->host);
        break;
      case S: {
        int reuse = 1;
        CHECK_SYSCALL_OR_GOTO(
            cleanup, retval, false,
            setsockopt(this->sock, SOL_SOCKET, SO_REUSEADDR,
                       &reuse, sizeof(reuse)),
            "setsockopt(SO_REUSEADDR) failed for ", this->host);
        CHECK_SYSCALL_OR_WARN(
            rv = bind(this->sock, i->ai_addr, i->ai_addrlen),
            "warning: bind() failed for one of addresses for ", this->host);
        break;
      }
      default:
        assert(0);
    }

    if (rv != -1)
      break;

    COND_CHECK_SYSCALL_OR_WARN(
        this->sock, -1, close(this->sock),
        "warning: close() failed for one of addresses for ", this->host);
  }

  if (this->sock == -1) {
    fprintf(stderr, "failed to initialize connection for %s\n", this->host);
    retval = false;
    goto cleanup;
  }

  if (this->mode == S) {
    CHECK_SYSCALL_OR_GOTO(
        cleanup, retval, false,
        listen(this->sock, 1), "listen() failed for ", this->host);

    CHECK_SYSCALL_OR_GOTO(
        cleanup, retval, false,
        this->client_sock = accept(this->sock, NULL, NULL),
        "accept() failed for ", this->host);
  }

  assert(block_size <= INT_MAX);
  const int optvalue = block_size;
  CHECK_SYSCALL_OR_WARN(
      setsockopt(this->mode == S ? this->client_sock : this->sock,
                 SOL_SOCKET,
                 this->mode == S ? SO_SNDBUFFORCE : SO_RCVBUFFORCE,
                 &optvalue, sizeof(optvalue)),
      "warning: setsockopt(*_BUFFORCE) failed for ", this->host);

cleanup:
  freeaddrinfo(result);
  return retval;
}

static const char *name(void *data) {
  GET(struct data, this, data);
  return this->host;
}

static void destroy(void *data) {
  GET(struct data, this, data);
  if (this->mode == S)
    COND_CHECK_SYSCALL_OR_WARN(
        this->client_sock, -1, close(this->client_sock),
        "failed to close client socket for ", this->host);
  COND_CHECK_SYSCALL_OR_WARN(this->sock, -1, close(this->sock),
                             "failed to close socket for ", this->host);
  free(data);
}

static uint32_t get_epoll_event(void *data) {
  GET(struct data, this, data);
  return (this->mode == R) ? EPOLLIN : EPOLLOUT;
}

static int get_fd(void *data) {
  GET(struct data, this, data);
  return (this->mode == R) ? this->sock : this->client_sock;
}

static bool would_block(int rv) {
  return rv == -1 && (errno == EAGAIN || errno == EWOULDBLOCK);
}

static ssize_t produce(void *data, void *buf, size_t count, bool *eof) {
  GET(struct data, this, data);
  ssize_t rv = recv(this->sock, buf, count, MSG_DONTWAIT);
  if (would_block(rv)) {
    *eof = false;
    return 0;
  } else if (rv == 0) {
    *eof = true;
    return 0;
  }

  CHECK_SYSCALL_OR_RETURN(-1, rv, "recv() failed for ", this->host);
  return rv;
}

static ssize_t produce_signal(void *data, bool *eof) {
  GET(struct data, this, data);
  char unused;
  ssize_t rv = recv(this->sock, &unused, 1, MSG_PEEK);
  CHECK_OR_RETURN(
      -1, !would_block(rv),
      "recv() blocked when after notification, shouldn't happen");
  CHECK_SYSCALL_OR_RETURN(-1, rv, "recv(MSG_PEEK) failed for ", this->host);
  *eof = (rv == 0);
  return 0;
}

static ssize_t consume_signal(void *data) {
  return 0;
}

static ssize_t consume(void *data, void *buf, size_t count) {
  GET(struct data, this, data);
  ssize_t rv = send(this->client_sock, buf, count, MSG_DONTWAIT);
  if (would_block(rv))
    return 0;

  CHECK_SYSCALL_OR_RETURN(-1, rv, "send() failed for ", this->host);
  return rv;
}

static const struct producer_ops recv_ops = {
  .init             = init,
  .name             = name,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
  .produce          = produce,
  .signal           = produce_signal,
};

static const struct consumer_ops send_ops = {
  .init             = init,
  .name             = name,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
  .consume          = consume,
  .signal           = consume_signal,
};

static struct data *construct(const char *spec, int mode) {
  assert(spec);
  struct data *data = malloc(sizeof(struct data) + strlen(spec) + 1);

  if (data) {
    data->sock = -1;
    data->client_sock = -1;

    data->mode = mode;
    char *colon = strchr(spec, ':');
    if (colon) {
      strncpy(data->host, spec, colon - spec);
      if (strlen(spec) > strlen(data->host) + 1 + PORT_MAX_CHARS) {
        fputs("port too long", stderr);
        goto cleanup;
      }
      strncpy(data->port, colon+1, PORT_MAX_CHARS+1);
    } else {
      strcpy(data->host, spec);
      strcpy(data->port, DEFAULT_PORT);
    }
  }

  return data;

cleanup:
  free(data);
  return NULL;
}

struct producer get_socket_reader(const char *spec) {
  return (struct producer) {&recv_ops, construct(spec, R)};
}

struct consumer get_socket_writer(const char *spec) {
  return (struct consumer) {&send_ops, construct(spec, S)};
}
