#include "file.h"
#include "struct.h"

#include <assert.h>
#include <fcntl.h>
#include <linux/aio_abi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/syscall.h>
#include <unistd.h>

struct file_data {
  int fd;
  int afd;
  aio_context_t ctx;
  struct iocb cb;

  size_t offset;
  size_t pending_size;
  enum { R, W } mode;
  char filename[];
};

bool file_init(void *data) {
  GET(struct file_data, this, data);
  int mode = (this->mode == R) ? O_RDONLY : O_WRONLY | O_CREAT;
  mode |= O_NONBLOCK;
  if ((this->fd = open(this->filename, mode)) == -1) {
    fputs("failed to open ", stderr);
    perror(this->filename);
    return false;
  }

  if ((this->afd = eventfd(0, 0)) == -1) {
    fputs("failed to initalize eventfd for ", stderr);
    perror(this->filename);
    return false;
  }

  if (syscall(SYS_io_setup, 1, &this->ctx) == -1) {
    fputs("failed to initalize aio control block for ", stderr);
    perror(this->filename);
    return false;
  }

  this->cb.aio_fildes = this->fd;
  this->cb.aio_lio_opcode =
      (this->mode == R) ? IOCB_CMD_PREAD : IOCB_CMD_PWRITE;
  this->cb.aio_reqprio = 0;
  this->cb.aio_flags = IOCB_FLAG_RESFD;
  this->cb.aio_resfd = this->afd;

  return true;
}

void close_or_warn(int *fd, const char *filename, const char *msg) {
  if (*fd != -1) {
    if (close(*fd)) {
      fputs(msg, stderr);
      perror(filename);
    }
    *fd = -1;
  }
}

void file_destroy(void *data) {
  GET(struct file_data, this, data);

  this->offset = 0;
  this->pending_size = 0;
  memset(&this->cb, 0, sizeof(this->cb));

  if (this->ctx != 0) {
    if (syscall(SYS_io_destroy, this->ctx) == -1) {
        fputs("failed to close aio control block for ", stderr);
        perror(this->filename);
    }
    this->ctx = 0;
  }

  close_or_warn(&this->afd, this->filename, "failed to close eventfd for ");
  close_or_warn(&this->fd, this->filename, "failed to close ");

  free(data);
}

int file_get_fd(void *data) {
  GET(struct file_data, this, data);
  return this->afd;
}

ssize_t file_enqueue(void *data, void *buf, size_t count) {
  GET(struct file_data, this, data);

  static_assert(
      sizeof(uint64_t) >= sizeof(void *) && sizeof(uint64_t) >= sizeof(size_t),
      "can't use io_submit on this platform");
  this->cb.aio_buf = (uint64_t) buf;
  this->cb.aio_nbytes = count;
  this->cb.aio_offset = this->offset;

  // TODO: check file size

  struct iocb *cbs = {&this->cb};
  if (syscall(SYS_io_submit, this->ctx, 1, &cbs) == -1) {
    fputs("failed to submit aio request for ", stderr);
    perror(this->filename);
    return -1;
  }

  this->pending_size = count;

  return count;
}

bool file_signal(void *data) {
  GET(struct file_data, this, data);

  struct io_event event;
  if (syscall(SYS_io_getevents, this->ctx, 1, 1, &event, NULL) != 1) {
    fputs("failed to get completed aio events for ", stderr);
    perror(this->filename);
    return false;
  }

  this->offset += this->pending_size;
  this->pending_size = 0;
  return true;
}

static const struct producer_ops input_ops = {
  .init    = file_init,
  .destroy = file_destroy,

  .get_fd  = file_get_fd,
  .produce = file_enqueue,
  .signal  = file_signal,
};

static const struct consumer_ops output_ops = {
  .init    = file_init,
  .destroy = file_destroy,

  .get_fd  = file_get_fd,
  .consume = file_enqueue,
  .signal  = file_signal,
};

struct file_data *get_file_data(const char *filename, int mode) {
  struct file_data *data = malloc(
      sizeof(struct file_data) + strlen(filename) + 1);

  data->fd = -1;
  data->afd = -1;
  data->ctx = 0;
  memset(&data->cb, 0, sizeof(data->cb));

  data->offset = 0;
  data->pending_size = 0;
  data->mode = mode;
  strcpy(data->filename, filename);

  return data;
}

struct producer get_file_reader(const char *filename) {
  return (struct producer) {&input_ops, get_file_data(filename, R)};
}

struct consumer get_file_writer(const char *filename) {
  return (struct consumer) {&output_ops, get_file_data(filename, W)};
}
