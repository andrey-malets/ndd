#include "file.h"
#include "struct.h"

#include <aio.h>
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>

struct file_data {
  int fd;
  struct aiocb ios[2];

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
  if ((this->fd = open(this->filename, mode)) == -1) {
    fputs("failed to open ", stderr);
    perror(this->filename);
    return false;
  }

  return true;
}

void file_destroy(void *data) {
  GET(struct file_data, this, data);
  if (this->fd != -1) {
    if (close(this->fd)) {
      fputs("failed to close ", stderr);
      perror(this->filename);
    }
    this->fd = -1;
  }
}

static const struct producer_ops input_ops = {
  .get_fd = file_get_fd,
  .init   = file_init,
  .destroy = file_destroy
};

static const struct consumer_ops output_ops = {
  .get_fd = file_get_fd,
  .init   = file_init,
  .destroy = file_destroy
};

struct file_data *get_file_data(const char *filename, int mode) {
  struct file_data *data = malloc(
      sizeof(struct file_data) + strlen(filename) + 1);

  memset(data, 0, sizeof(struct file_data));
  data->fd = -1;
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
