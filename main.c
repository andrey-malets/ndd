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

static bool init_producer(struct producer *producer,
                          struct producer (*fn)(const char*), const char *arg) {
  CHECK(is_empty_producer(producer),
        ERROR("there can only be one producer"), return false);

  *producer = fn(arg);
  CHECK(!is_empty_producer(producer),
        ERROR("failed to construct producer"), return false);
  return true;
}

static bool add_consumer(struct consumer *consumers, size_t *num_consumers,
                         struct consumer (*fn)(const char*), const char *arg) {
  CHECK(*num_consumers != MAX_CONSUMERS,
        ERROR("too many consumers"), return false);

  consumers[(*num_consumers)++] = fn(arg);
  CHECK(!is_empty_consumer(&consumers[*num_consumers-1]),
        ERROR("failed to construct consumer"), return false);
  return true;
}

static bool strtoll_overflew(long long value) {
  return (value == LLONG_MIN || value == LLONG_MAX) && errno == ERANGE;
}

static bool strtol_overflew(long value) {
  return (value == LONG_MIN || value == LONG_MAX) && errno == ERANGE;
}

int main(int argc, char *argv[]) {
  int rv = 0;

  struct state state = EMPTY_STATE;

  struct stats stats = EMPTY_STATS;
  const char *stats_filename = NULL;

  size_t buffer_size = DEFAULT_BUFFER_SIZE;
  size_t block_size = DEFAULT_BLOCK_SIZE;
  long sleep_ms = DEFAULT_SLEEP_MS;

  long long raw_size;
  static_assert(sizeof(size_t) == sizeof(long long),
                "can't manipulate buffer sizes on this platform");

#define FAIL_IF_NOT(cond, alert) CHECK(cond, alert, GOTO_WITH(cleanup, rv, 1))

  for (int opt; (opt = getopt(argc, argv, "B:b:i:o:r:s:S:t:")) != -1;) {
    switch (opt) {
    case 'B':
    case 'b': {
      char *end = NULL;
      raw_size = strtoll(optarg, &end, 10);
      FAIL_IF_NOT(*end == 0 && raw_size > 0ll && !strtoll_overflew(raw_size),
                  ERROR("can't read buffer/block size"));
      (opt == 'B') ? (buffer_size = raw_size) : (block_size = raw_size);
      break;
    }
    case 'i':
      FAIL_IF_NOT(init_producer(&state.producer, get_file_reader, optarg), ;);
      break;
    case 'o':
      FAIL_IF_NOT(add_consumer(state.consumers, &state.num_consumers,
                               get_file_writer, optarg), ;);
      break;
    case 'r':
      FAIL_IF_NOT(init_producer(&state.producer, get_socket_reader, optarg), ;);
      break;
    case 's':
      FAIL_IF_NOT(add_consumer(state.consumers, &state.num_consumers,
                               get_socket_writer, optarg), ;);
      break;
    case 'S':
      stats_filename = optarg;
      state.stats = &stats;
      break;
    case 't': {
      char *end = NULL;
      long raw_sleep;
      raw_sleep = strtol(optarg, &end, 10);
      FAIL_IF_NOT(*end == 0 && raw_sleep >= 0l && !strtol_overflew(raw_sleep) &&
                      raw_sleep < INT_MAX,
                  ERROR("can't read sleep timeout"));
      sleep_ms = raw_sleep;
      break;
    }
    }
  }

  FAIL_IF_NOT(buffer_size > block_size,
              ERROR("buffer size should be greather than block size"));
  FAIL_IF_NOT(buffer_size % block_size == 0,
              ERROR("buffer size should be a multiple of block size"));

  FAIL_IF_NOT(!is_empty_producer(&state.producer),
              ERROR("please specify a producer"));

  FAIL_IF_NOT(state.num_consumers > 0,
              ERROR("please specify at least one consumer"));

  for (size_t i = 0; i != state.num_consumers; ++i)
    FAIL_IF_NOT(CALL(state.consumers[i], init, block_size),
                ERROR("failed to initialize consumer"));
  FAIL_IF_NOT(CALL(state.producer, init, block_size),
              ERROR("failed to initialize producer"));

  FAIL_IF_NOT(transfer(buffer_size, block_size, sleep_ms, &state),
              ERROR("transfer failed"));

  if (stats_filename)
    FAIL_IF_NOT(dump_stats(&state, stats_filename),
                ERROR("failed to dump stats"));

cleanup:
  if (!is_empty_producer(&state.producer))
    CALL0(state.producer, destroy);

  for (size_t i = state.num_consumers; i--;)
    if (!is_empty_consumer(&state.consumers[i]))
      CALL0(state.consumers[i], destroy);
  return rv;
}
