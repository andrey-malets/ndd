#include "defaults.h"
#include "engine.h"
#include "file.h"
#include "macro.h"
#include "pipe.h"
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
                         struct consumer (*fn)(const char*, size_t),
                         size_t lo_watermark, const char *arg) {
  CHECK(*num_consumers != MAX_CONSUMERS,
        ERROR("too many consumers"), return false);

  consumers[(*num_consumers)++] = fn(arg, lo_watermark);
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

static bool size_overflew(size_t value) {
  return sizeof(size_t) == sizeof(long) ?
      strtol_overflew(value) :
      strtoll_overflew(value);
}

int main(int argc, char *argv[]) {
  int rv = 0;

  struct state state = EMPTY_STATE;

  struct stats stats = EMPTY_STATS;
  const char *stats_filename = NULL;

  size_t buffer_size = DEFAULT_BUFFER_SIZE;
  size_t block_size = DEFAULT_BLOCK_SIZE;
  size_t lo_watermark = DEFAULT_LO_WATERMARK;
  size_t raw_size;
  static_assert(sizeof(size_t) == sizeof(long long) ||
                sizeof(size_t) == sizeof(long),
                "can't manipulate buffer sizes on this platform");

#define FAIL_IF_NOT(cond, alert) CHECK(cond, alert, GOTO_WITH(cleanup, rv, 1))

  for (int opt; (opt = getopt(argc, argv, "B:b:i:o:I:O:r:s:S:")) != -1;) {
    switch (opt) {
    case 'B':
    case 'b': {
      char *end = NULL;
      raw_size = (sizeof(size_t) == sizeof(long)) ?
          strtol(optarg, &end, 10) :
          strtoll(optarg, &end, 10);
      FAIL_IF_NOT(*end == 0 && raw_size > 0ll && !size_overflew(raw_size),
                  ERROR("can't read buffer/block size"));
      (opt == 'B') ? (buffer_size = raw_size) : (block_size = raw_size);
      break;
    }
    case 'S':
      stats_filename = optarg;
      state.stats = &stats;
      break;
#define PRODUCER(letter, func) \
    case letter: \
      FAIL_IF_NOT(init_producer(&state.producer, func, optarg), ;); \
      break
#define CONSUMER(letter, func) \
    case letter: \
      FAIL_IF_NOT(add_consumer(state.consumers, &state.num_consumers, \
                               func, lo_watermark, optarg), ;); \
      break
    PRODUCER('i', get_file_reader);   CONSUMER('o', get_file_writer);
    PRODUCER('I', get_pipe_reader);   CONSUMER('O', get_pipe_writer);
    PRODUCER('r', get_socket_reader); CONSUMER('s', get_socket_writer);
    }
  }

  FAIL_IF_NOT(buffer_size > block_size,
              ERROR("buffer size should be greather than block size"));
  FAIL_IF_NOT(buffer_size % block_size == 0,
              ERROR("buffer size should be a multiple of block size"));
  FAIL_IF_NOT(lo_watermark <= block_size,
              ERROR("lo watermark must not be greather than block size"));

  FAIL_IF_NOT(!is_empty_producer(&state.producer),
              ERROR("please specify a producer"));

  FAIL_IF_NOT(state.num_consumers > 0,
              ERROR("please specify at least one consumer"));

  for (size_t i = 0; i != state.num_consumers; ++i)
    FAIL_IF_NOT(CALL(state.consumers[i], init, block_size),
                ERROR("failed to initialize consumer"));
  FAIL_IF_NOT(CALL(state.producer, init, block_size),
              ERROR("failed to initialize producer"));

  FAIL_IF_NOT(transfer(buffer_size, block_size, &state),
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
