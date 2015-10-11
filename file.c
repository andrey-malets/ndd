#include "file.h"
#include "struct.h"

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

  enum { R, W } mode;
  char filename[];
};

int file_get_fd(void *data) {
  GET(struct file_data, this, data);
  return this->fd;
}

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

static const struct producer_ops input_ops = {
  .get_fd  = file_get_fd,
  .init    = file_init,
  .destroy = file_destroy
};

static const struct consumer_ops output_ops = {
  .get_fd  = file_get_fd,
  .init    = file_init,
  .destroy = file_destroy
};

struct file_data *get_file_data(const char *filename, int mode) {
  struct file_data *data = malloc(
      sizeof(struct file_data) + strlen(filename) + 1);

  data->fd = -1;
  data->afd = -1;
  data->ctx = 0;

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
