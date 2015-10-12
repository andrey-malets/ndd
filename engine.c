#include "engine.h"
#include "macro.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <unistd.h>

bool transfer(size_t buffer_size, size_t block_size,
              struct producer producer,
              struct consumer *consumers,
              size_t num_consumers) {
  bool rv = true;
  char *buffer = NULL;

  int epoll_fd = -1;
  // TODO: change this into check for errno
  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, false, "failed to create epoll fd",
                         (epoll_fd = epoll_create(1)) != -1);

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, false, "can't allocate memory for buffer",
                         buffer = malloc(buffer_size));

  struct epoll_event ev;
  int producer_fd = CALL0(producer, get_fd);
  ev.events = CALL0(producer, get_epoll_event);
  ev.data.fd = producer_fd;

  CHECK_OR_GOTO_WITH_MSG(
      cleanup, rv, false, "can't add producer fd to epoll",
      epoll_ctl(epoll_fd, EPOLL_CTL_ADD, producer_fd, &ev) != -1);

  // TODO: do actual processing.

cleanup:
  free(buffer);
  COND_CHECK_SYSCALL_OR_WARN(epoll_fd, -1, close(epoll_fd),
                             "failed to close ", "epoll fd");
  return rv;
}
