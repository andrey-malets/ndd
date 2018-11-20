#include "defaults.h"
#include "macro.h"
#include "socket.h"
#include "struct.h"
#include "util.h"

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

#define PORT_MAX_CHARS 5

#define CHECK_OR_WARN(value, msg, act) \
  CHECK(SYSCALL(value), \
        PERROR1("warning: " msg " failed for one of addresses for", \
                this->host), \
        act)

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

static int try_connect(struct data *this, struct addrinfo *ai) {
  int rv = -1;
  for (size_t i = 0; i != arraysize(CONNECT_BACKOFF); ++i) {
    // ignore signals
    sleep(CONNECT_BACKOFF[i]);

    rv = connect(this->sock, ai->ai_addr, ai->ai_addrlen);

    if (refused(rv))
      continue;
    CHECK_OR_WARN(rv, "connect()", ;);
    return rv;
  }

  CHECK_OR_WARN(rv, "connect()", ;);
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

const static struct in_addr IPv4_LOCALHOST = { 0x7f000001 };
const static struct in6_addr IPv6_LOCALHOST = { {
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x00,
  0x00, 0x00, 0x00, 0x01,
} };

static bool is_localhost(const struct addrinfo *ai) {
  switch (ai->family) {
    case AF_INET:
      return memcmp(ai->ai_addr, IPv4_LOCALHOST, sizeof(IPv4_LOCALHOST)) == 0;
    case AF_INET6:
      return memcmp(ai->ai_addr, IPv6_LOCALHOST, sizeof(IPv6_LOCALHOST)) == 0;
    return false;
  }
}

static bool init(void *data, size_t block_size) {
  GET(struct data, this, data);

  bool retval = true;

  struct addrinfo hints = get_hints(this->mode);
  struct addrinfo *result;

  int gai_rv = -1;
  CHECK((gai_rv = getaddrinfo(strlen(this->host) ? this->host : NULL, this->port,
                              &hints, &result)) == 0,
        GAI_PERROR1("getaddrinfo() failed for", this->host, gai_rv),
        return false);

  for (struct addrinfo *i = result; i != NULL; i = i->ai_next) {
    if (is_localhost(i))
      continue;

    CHECK_OR_WARN(
        (this->sock = socket(i->ai_family, i->ai_socktype, i->ai_protocol)),
        "socket()", ;);

    if (this->sock == -1)
      continue;

    int rv = -1;
    switch (this->mode) {
      case R:
        rv = try_connect(this, i);
        break;
      case S: {
        int reuse = 1;
        CHECK_OR_WARN(setsockopt(this->sock, SOL_SOCKET, SO_REUSEADDR,
                                 &reuse, sizeof(reuse)),
                      "setsockopt(SO_REUSEADDR)", goto end);
        CHECK_OR_WARN(rv = bind(this->sock, i->ai_addr, i->ai_addrlen),
                      "bind()", goto end);
        break;
      }
      default:
        assert(0);
    }

    if (rv != -1)
      break;

end:
    COND_CHECK(this->sock, -1, SYSCALL(close(this->sock)),
               PERROR1("warning: close() failed for one of addresses for",
                       this->host));
  }

  CHECK(this->sock != -1,
        fprintf(stderr, "failed to initialize connection for %s\n", this->host),
        GOTO_WITH(cleanup, retval, false));

  if (this->mode == S) {
    CHECK(SYSCALL(listen(this->sock, 1)),
          PERROR1("listen() failed for", this->host),
          GOTO_WITH(cleanup, retval, false));

    CHECK(SYSCALL(this->client_sock = accept(this->sock, NULL, NULL)),
          PERROR1("accept() failed for", this->host),
          GOTO_WITH(cleanup, retval, false));
  }

  CHECK(block_size <= INT_MAX, ERROR("too big block size"), goto cleanup);
  const int optvalue = block_size;
  CHECK_OR_WARN(setsockopt(this->mode == S ? this->client_sock : this->sock,
                           SOL_SOCKET,
                           this->mode == S ? SO_SNDBUFFORCE : SO_RCVBUFFORCE,
                           &optvalue, sizeof(optvalue)),
                "setsockopt(*_BUFFORCE)", ;);
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
    COND_CHECK(
        this->client_sock, -1,
        SYSCALL(close(this->client_sock)),
        PERROR1("failed to close client socket for", this->host));
  COND_CHECK(this->sock, -1, SYSCALL(close(this->sock)),
             PERROR1("failed to close socket for", this->host));
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

  CHECK(SYSCALL(rv), PERROR1("recv() failed for", this->host), return -1);
  return rv;
}

static ssize_t produce_signal(void *data, bool *eof) {
  GET(struct data, this, data);
  char unused;
  ssize_t rv = recv(this->sock, &unused, 1, MSG_PEEK);
  CHECK(!would_block(rv),
        ERROR("recv() blocked when after notification, shouldn't happen"),
        return -1);
  CHECK(SYSCALL(rv),
        PERROR1("recv(MSG_PEEK) failed for", this->host), return -1);
  *eof = (rv == 0);
  return 0;
}

static ssize_t consume(void *data, void *buf, size_t count) {
  GET(struct data, this, data);
  ssize_t rv = send(this->client_sock, buf, count, MSG_DONTWAIT);
  if (would_block(rv))
    return 0;

  CHECK(SYSCALL(rv), PERROR1("send() failed for", this->host), return -1);
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
  .get_lo_watermark = get_zero_lo_watermark,
  .consume          = consume,
  .signal           = zero_consume_signal,
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

struct consumer get_socket_writer(const char *spec, size_t lo_watermark) {
  return (struct consumer) {&send_ops, construct(spec, S)};
}

#undef CHECK_OR_WARN
