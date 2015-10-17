#include "engine.h"
#include "macro.h"
#include "stats.h"
#include "struct.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/epoll.h>
#include <time.h>
#include <unistd.h>

static_assert(sizeof(uint64_t) >= sizeof(size_t),
              "can't express sizes on this platform");

uint64_t min(uint64_t a, uint64_t b) {
  return a < b ? a : b;
}

struct entry {
  enum { P, C } type;
  union {
    struct producer *producer;
    struct consumer *consumer;
  };
  uint64_t offset;
  bool was_busy;
  bool busy;
};

uint64_t min_offset(struct entry *index, size_t num_consumers) {
  uint64_t rv = UINT64_MAX;
  for (size_t i = 0; i != num_consumers; ++i)
    rv = min(rv, index[1+i].offset);
  return rv;
}

static bool change_wait(int epoll_fd, bool add,
                        int fd, uint32_t events, void *ptr) {
  struct epoll_event ev = { .events = events, .data.ptr = ptr };
  CHECK(SYSCALL(epoll_ctl(epoll_fd, add ? EPOLL_CTL_ADD : EPOLL_CTL_DEL,
                          fd, &ev)),
        perror("epoll_ctl() failed"), DO_RETURN(false));

  return true;
}

static void prepare(struct state *const state, struct entry *const index) {
  index[0] = (struct entry) {
    .type = P,
    .producer = &state->producer,
    .offset = 0,
    .was_busy = false,
    .busy = false
  };

  for (size_t i = 0; i != state->num_consumers; ++i) {
    index[1+i] = (struct entry) {
      .type = C,
      .consumer = &state->consumers[i],
      .offset = 0,
      .was_busy = false,
      .busy = false
    };
  }
}

bool transfer(size_t buffer_size, size_t block_size,
              int sleep_ms, struct state *const state) {
  bool rv = true;
  struct entry index[1+MAX_CONSUMERS];
  struct epoll_event events[1+MAX_CONSUMERS];
  char *buffer = NULL;
  int epoll_fd = -1;
  bool eof = false;
  size_t waiting = 0;

  CHECK_SYSCALL_OR_GOTO(cleanup, rv, false, epoll_fd = epoll_create(1),
                        "failed to create ", "epoll fd");

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, false, "can't allocate memory for buffer",
                         buffer = malloc(buffer_size));

  prepare(state, index);

  for (;;) {
    INC(state->stats, total_cycles);

    if (waiting) {
      INC(state->stats, waited_cycles);
      int num_events;
      CHECK_SYSCALL_OR_GOTO(
          cleanup, rv, false,
          num_events = epoll_wait(epoll_fd, events, waiting, sleep_ms),
          "epoll_wait ", "failed");
      for (int i = 0; i != num_events; ++i) {
        struct entry *entry = events[i].data.ptr;
        assert(entry->busy);
        ssize_t moved;
        switch (entry->type) {
        case P:
          moved = CALL(*entry->producer, signal, &eof);
          break;
        case C:
          moved = CALL0(*entry->consumer, signal);
          break;
        default:
          assert(0);
          break;
        }
        CHECK_OR_GOTO(cleanup, rv, false, moved != -1);
        entry->offset += moved;
        entry->busy = false;
        waiting -= 1;
      }
    }

    {
      uint64_t begin = index[0].offset;
      uint64_t end = min_offset(index, state->num_consumers);
      assert(begin >= end);

      if (begin == end && eof)
        break;

      uint64_t sbegin = begin % buffer_size;
      uint64_t send = end % buffer_size;

      if (!index[0].busy) {
        uint64_t offset = 0, size = 0;
        if (sbegin > send) {
          offset = sbegin;
          size = buffer_size - sbegin;
        } else if (sbegin < send) {
          offset = sbegin;
          size = send - sbegin;
        } else if (begin == end) {
          offset = sbegin;
          size = buffer_size - sbegin;
        }

        if (size) {
          ssize_t produced;
          CHECK_OR_GOTO(
              cleanup, rv, false,
              (produced = CALL(*index[0].producer, produce,
                  buffer+offset, min(block_size, size), &eof)) != -1);

          waiting += (index[0].busy = (produced == 0));
          index[0].offset += produced;
        } else {
          INC(state->stats, buffer_overruns);
          for (size_t i = 0; i != state->num_consumers; ++i) {
            if (index[1+i].offset == end)
              INC(state->stats, consumer_slowdowns[i]);
          }
        }

        if (index[0].was_busy != index[0].busy) {
          CHECK(change_wait(epoll_fd, index[0].busy,
                            CALL0(*index[0].producer, get_fd),
                            CALL0(*index[0].producer, get_epoll_event),
                            &index[0]),
                ERROR("failed to change waits"),
                DO_GOTO(cleanup, rv, false));
          index[0].was_busy = index[0].busy;
        }
      }
    }

    {
      uint64_t begin = index[0].offset;
      for (size_t i = 0; i != state->num_consumers; ++i) {
        if (!index[1+i].busy) {
          uint64_t end = index[1+i].offset;
          assert(begin >= end);

          uint64_t sbegin = begin % buffer_size;
          uint64_t send = end % buffer_size;

          uint64_t offset = 0, size = 0;
          if (sbegin > send) {
            offset = send;
            size = sbegin - send;
          } else if (sbegin < send) {
            offset = send;
            size = buffer_size - send;
          } else if (begin > end) {
            offset = send;
            size = buffer_size - send;
          }

          if (size) {
            ssize_t consumed;
            CHECK_OR_GOTO(
                cleanup, rv, false,
                (consumed = CALL(*index[1+i].consumer, consume,
                                 buffer+offset, min(block_size, size))) != -1);

            waiting += (index[1+i].busy = (consumed == 0));
            index[1+i].offset += consumed;
          } else {
            INC(state->stats, buffer_underruns);
          }

          if (index[1+i].was_busy != index[1+i].busy) {
            CHECK(change_wait(epoll_fd, index[1+i].busy,
                              CALL0(*index[1+i].consumer, get_fd),
                              CALL0(*index[1+i].consumer, get_epoll_event),
                              &index[1+i]),
                  ERROR("failed to change waits "),
                  DO_GOTO(cleanup, rv, false));
            index[1+i].was_busy = index[1+i].busy;
          }
        }
      }
    }
  }

cleanup:
  free(buffer);
  COND_CHECK_SYSCALL_OR_WARN(epoll_fd, -1, close(epoll_fd),
                             "failed to close ", "epoll fd");
  return rv;
}
