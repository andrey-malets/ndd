#include "file.h"
#include "macro.h"
#include "struct.h"

#include <assert.h>
#include <fcntl.h>
#include <linux/aio_abi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>

struct data {
  int fd;
  int afd;
  aio_context_t ctx;
  struct iocb cb;

  uint64_t offset;
  enum { R, W } mode;
  char filename[];
};

static bool init(void *data) {
  GET(struct data, this, data);
  int mode = (this->mode == R) ? O_RDONLY : O_WRONLY | O_CREAT;
  mode |= O_NONBLOCK;
  CHECK_SYSCALL_OR_RETURN(false, this->fd = open(this->filename, mode, S_IWUSR),
                          "failed to open ", this->filename);

  CHECK_SYSCALL_OR_RETURN(false, this->afd = eventfd(0, 0),
                          "failed to initialize eventfd for ", this->filename);

  CHECK_SYSCALL_OR_RETURN(false, syscall(SYS_io_setup, 1, &this->ctx),
                          "failed to initalize aio control block for ",
                          this->filename);

  this->cb.aio_fildes = this->fd;
  this->cb.aio_lio_opcode =
      (this->mode == R) ? IOCB_CMD_PREAD : IOCB_CMD_PWRITE;
  this->cb.aio_reqprio = 0;
  this->cb.aio_flags = IOCB_FLAG_RESFD;
  this->cb.aio_resfd = this->afd;

  return true;
}

static void destroy(void *data) {
  GET(struct data, this, data);

  this->offset = 0;
  memset(&this->cb, 0, sizeof(this->cb));

  COND_CHECK_SYSCALL_OR_WARN(
      this->ctx, 0, syscall(SYS_io_destroy, this->ctx),
      "failed to close aio control block for ", this->filename);

  COND_CHECK_SYSCALL_OR_WARN(this->afd, -1, close(this->afd),
                             "failed to close eventfd for ", this->filename);

  COND_CHECK_SYSCALL_OR_WARN(this->fd, -1, close(this->fd),
                             "failed to close ", this->filename);

  free(data);
}

static uint32_t get_epoll_event(void *data) {
  return EPOLLIN;
}

static int get_fd(void *data) {
  GET(struct data, this, data);
  return this->afd;
}

static ssize_t enqueue(void *data, void *buf, size_t count, bool *eof) {
  GET(struct data, this, data);

  static_assert(
      sizeof(uint64_t) >= sizeof(void *) && sizeof(uint64_t) >= sizeof(size_t),
      "can't use io_submit on this platform");
  this->cb.aio_buf = (uint64_t) buf;
  this->cb.aio_nbytes = count;
  this->cb.aio_offset = this->offset;

  struct iocb *cbs = {&this->cb};
  CHECK_SYSCALL_OR_RETURN(-1, syscall(SYS_io_submit, this->ctx, 1, &cbs),
                          "failed to submit aio request for ", this->filename);
  // Here we don't know.
  *eof = false;
  return 0;
}

static ssize_t consume(void *data, void *buf, size_t count) {
  bool unused;
  return enqueue(data, buf, count, &unused);
}

static ssize_t signal(void *data, bool *eof) {
  GET(struct data, this, data);

  struct io_event event;
  CHECK_SYSCALL_OR_RETURN(
      false, syscall(SYS_io_getevents, this->ctx, 1, 1, &event, NULL),
      "failed to get completed aio events for ", this->filename);

  this->offset += event.res;
  *eof = (event.res == 0);
  return event.res;
}

static ssize_t consume_signal(void *data) {
  bool unused;
  return signal(data, &unused);
}

static const struct producer_ops input_ops = {
  .init             = init,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
  .produce          = enqueue,
  .signal           = signal,
};

static const struct consumer_ops output_ops = {
  .init             = init,
  .destroy          = destroy,

  .get_epoll_event  = get_epoll_event,
  .get_fd           = get_fd,
  .consume          = consume,
  .signal           = consume_signal,
};

static struct data *construct(const char *filename, int mode) {
  assert(filename);
  struct data *data = malloc(sizeof(struct data) + strlen(filename) + 1);

  if (data) {
    data->fd = -1;
    data->afd = -1;
    data->ctx = 0;
    memset(&data->cb, 0, sizeof(data->cb));

    data->offset = 0;
    data->mode = mode;
    strcpy(data->filename, filename);
  }

  return data;
}

struct producer get_file_reader(const char *filename) {
  return (struct producer) {&input_ops, construct(filename, R)};
}

struct consumer get_file_writer(const char *filename) {
  return (struct consumer) {&output_ops, construct(filename, W)};
}
