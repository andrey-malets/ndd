#include "defaults.h"
#include "engine.h"
#include "file.h"
#include "macro.h"
#include "socket.h"
#include "stats.h"
#include "struct.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

int init_producer(struct producer *producer,
                  struct producer (*fn)(const char*),
                  const char *arg) {
  CHECK_OR_RETURN(0, is_empty_producer(producer),
                  "there can only be one producer");

  *producer = fn(arg);
  CHECK_OR_RETURN(0, !is_empty_producer(producer),
                  "failed to construct producer");
  return 1;
}

int add_consumer(struct consumer *consumers,
                 size_t *num_consumers,
                 struct consumer (*fn)(const char*),
                 const char *arg) {
  CHECK_OR_RETURN(0, *num_consumers != MAX_CONSUMERS, "too many consumers");

  consumers[(*num_consumers)++] = fn(arg);
  CHECK_OR_RETURN(0, !is_empty_consumer(&consumers[*num_consumers-1]),
                  "failed to construct consumer");
  return 1;
}

static bool strtoll_overflew(long long value) {
  return (value == LLONG_MIN || value == LLONG_MAX) && errno == ERANGE;
}

static bool strtol_overflew(long value) {
  return (value == LONG_MIN || value == LONG_MAX) && errno == ERANGE;
}

int main(int argc, char *argv[]) {
  int rv = 0;

  struct state state = {{0, 0}, 0, {{0, 0}}, NULL};

  struct stats stats = {0, 0, 0, 0, {0}};
  const char *stats_filename = NULL;

  size_t buffer_size = DEFAULT_BUFFER_SIZE;
  size_t block_size = DEFAULT_BLOCK_SIZE;
  long sleep_us = DEFAULT_SLEEP_US;

  long long raw_size;
  static_assert(sizeof(size_t) == sizeof(long long),
                "can't manipulate buffer sizes on this platform");

  for (int opt; (opt = getopt(argc, argv, "B:b:i:o:r:s:S:t:")) != -1;) {
    switch (opt) {
    case 'B':
    case 'b': {
      char *end = NULL;
      raw_size = strtoll(optarg, &end, 10);
      CHECK_OR_GOTO_WITH_MSG(
          cleanup, rv, 1, "can't read buffer/block size",
          *end == 0 && raw_size > 0ll && !strtoll_overflew(raw_size));
      (opt == 'B') ? (buffer_size = raw_size) : (block_size = raw_size);
      break;
    }
    case 'i':
      CHECK_OR_GOTO(cleanup, rv, 1,
                    init_producer(&state.producer, get_file_reader, optarg));
      break;
    case 'o':
      CHECK_OR_GOTO(cleanup, rv, 1,
                    add_consumer(state.consumers, &state.num_consumers,
                                 get_file_writer, optarg));
      break;
    case 'r':
      CHECK_OR_GOTO(cleanup, rv, 1,
                    init_producer(&state.producer, get_socket_reader, optarg));
      break;
    case 's':
      CHECK_OR_GOTO(cleanup, rv, 1,
                    add_consumer(state.consumers, &state.num_consumers,
                                 get_socket_writer, optarg));
      break;
    case 'S':
      stats_filename = optarg;
      state.stats = &stats;
      break;
    case 't': {
      char *end = NULL;
      long raw_sleep;
      raw_sleep = strtol(optarg, &end, 10);
      CHECK_OR_GOTO_WITH_MSG(
          cleanup, rv, 1, "can't read sleep timeout",
          *end == 0 && raw_sleep >= 0l && !strtol_overflew(raw_sleep));
      sleep_us = raw_sleep;
      break;
    }
    }
  }

  CHECK_OR_GOTO_WITH_MSG(
      cleanup, rv, 1, "buffer size should be greather than block size",
      buffer_size > block_size);
  CHECK_OR_GOTO_WITH_MSG(
      cleanup, rv, 1, "buffer size should be a multiple of block size",
      buffer_size % block_size == 0);

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "please specify a producer",
                         !is_empty_producer(&state.producer));
  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "please specify at least one consumer",
                         state.num_consumers > 0);

  for (size_t i = 0; i != state.num_consumers; ++i)
    CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "failed to initialize consumer",
                           CALL(state.consumers[i], init, block_size));

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "failed to initialize producer",
                         CALL(state.producer, init, block_size));

  CHECK_OR_GOTO_WITH_MSG(
      cleanup, rv, 1, "transfer failed",
      transfer(buffer_size, block_size, sleep_us, &state));

  if (stats_filename)
    CHECK_OR_GOTO_WITH_MSG(
        cleanup, rv, 1, "failed to dump stats",
        dump_stats(&state, stats_filename));

cleanup:
  if (!is_empty_producer(&state.producer))
    CALL0(state.producer, destroy);

  for (size_t i = state.num_consumers; i--;)
    if (!is_empty_consumer(&state.consumers[i]))
      CALL0(state.consumers[i], destroy);
  return rv;
}
