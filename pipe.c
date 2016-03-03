#include "pipe.h"
#include "macro.h"
#include "struct.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <unistd.h>

struct data {
  int fd;
  enum { R, W } mode;
  char filename[];
};

#define WITH_THIS(act) PERROR1("failed to " act " for", this->filename)

static bool init(void *data, size_t block_size) {
  GET(struct data, this, data);
  int mode = (this->mode == R) ? O_RDONLY : O_WRONLY | O_CREAT;
  mode |= (O_NONBLOCK | O_LARGEFILE);
  CHECK(SYSCALL(this->fd = open(this->filename, mode, S_IWUSR|S_IRUSR)),
        WITH_THIS("call open"), return false);
  return true;
}

static const char *name(void *data) {
  GET(struct data, this, data);
  return this->filename;
}

static void destroy(void *data) {
  GET(struct data, this, data);
  COND_CHECK(this->fd, -1, SYSCALL(close(this->fd)), WITH_THIS("call close"));
  free(data);
}

static uint32_t get_epoll_event(void *data) {
  GET(struct data, this, data);
  return (this->mode == R) ? EPOLLIN : EPOLLOUT;
}

static int get_fd(void *data) {
  GET(struct data, this, data);
  return this->fd;
}

static size_t get_lo_watermark(void *data) {
  return 0;
}

static bool would_block(int rv) {
  return rv == -1 && (errno == EAGAIN || errno == EWOULDBLOCK);
}

static ssize_t produce(void *data, void *buf, size_t count, bool *eof) {
  GET(struct data, this, data);
  ssize_t rv = read(this->fd, buf, count);
  if (would_block(rv)) {
    *eof = false;
    return 0;
  } else if (rv == 0) {
    *eof = true;
    return 0;
  }

  CHECK(SYSCALL(rv), WITH_THIS("read"), return -1);
  return rv;
}

static ssize_t produce_signal(void *data, bool *eof) {
  return 0;
}

static ssize_t consume(void *data, void *buf, size_t count) {
  GET(struct data, this, data);
  ssize_t rv = write(this->fd, buf, count);
  if (would_block(rv))
    return 0;

  CHECK(SYSCALL(rv), WITH_THIS("write"), return -1);
  return rv;
}

static ssize_t consume_signal(void *data) {
  return 0;
}

static const struct producer_ops input_ops = {
  .init             = init,
  .name             = name,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
  .produce          = produce,
  .signal           = produce_signal,
};

static const struct consumer_ops output_ops = {
  .init             = init,
  .name             = name,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
  .get_lo_watermark = get_lo_watermark,
  .consume          = consume,
  .signal           = consume_signal,
};

static struct data *construct(const char *filename, int mode) {
  assert(filename);
  struct data *data = malloc(sizeof(struct data) + strlen(filename) + 1);

  if (data) {
    data->fd = -1;
    data->mode = mode;
    strcpy(data->filename, filename);
  }

  return data;
}

struct producer get_pipe_reader(const char *filename) {
  return (struct producer) {&input_ops, construct(filename, R)};
}

struct consumer get_pipe_writer(const char *filename, size_t lo_watermark) {
  return (struct consumer) {&output_ops, construct(filename, W)};
}

#undef WITH_THIS
