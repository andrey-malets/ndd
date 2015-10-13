#include "engine.h"
#include "file.h"
#include "macro.h"
#include "socket.h"
#include "struct.h"

#include <assert.h>
#include <errno.h>
#include <limits.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#define DEFAULT_BUFFER_SIZE (1024*1024)
#define MAX_CONSUMERS 2

#define DEFAULT_BLOCK_SIZE (128*1024)

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

int main(int argc, char *argv[]) {
  int rv = 0;

  struct producer producer = {0, 0};
  struct consumer consumers[MAX_CONSUMERS] = {{0, 0}};
  size_t num_consumers = 0;

  size_t buffer_size = DEFAULT_BUFFER_SIZE;
  size_t block_size = DEFAULT_BLOCK_SIZE;

  long long raw_size;
  static_assert(sizeof(size_t) == sizeof(long long),
                "can't manipulate buffer sizes on this platform");

  for (int opt; (opt = getopt(argc, argv, "B:b:i:o:r:s:")) != -1;) {
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
                    init_producer(&producer, get_file_reader, optarg));
      break;
    case 'o':
      CHECK_OR_GOTO(cleanup, rv, 1,
          add_consumer(consumers, &num_consumers, get_file_writer, optarg));
      break;
    case 'r':
      CHECK_OR_GOTO(cleanup, rv, 1,
                    init_producer(&producer, get_socket_reader, optarg));
      break;
    case 's':
      CHECK_OR_GOTO(
          cleanup, rv, 1,
          add_consumer(consumers, &num_consumers, get_socket_writer, optarg));
      break;
    }
  }

  CHECK_OR_GOTO_WITH_MSG(
      cleanup, rv, 1, "buffer size should be greather than block size",
      buffer_size > block_size);
  CHECK_OR_GOTO_WITH_MSG(
      cleanup, rv, 1, "buffer size should be a multiple of block size",
      buffer_size % block_size == 0);

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "please specify a producer",
                         !is_empty_producer(&producer));
  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "please specify at least one consumer",
                         num_consumers > 0);

  for (size_t i = 0; i != num_consumers; ++i)
    CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "failed to initialize consumer",
                           CALL0(consumers[i], init));

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "failed to initialize producer",
                         CALL0(producer, init));

  CHECK_OR_GOTO_WITH_MSG(
      cleanup, rv, 1, "transfer failed",
      transfer(buffer_size, block_size, producer, consumers, num_consumers));

cleanup:
  if (!is_empty_producer(&producer))
    CALL0(producer, destroy);

  for (size_t i = num_consumers; i--;)
    if (!is_empty_consumer(&consumers[i]))
      CALL0(consumers[i], destroy);
  return rv;
}
