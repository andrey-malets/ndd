#include "file.h"
#include "socket.h"
#include "struct.h"

#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>

#define DEFAULT_BUFFER_SIZE (1024*1024)
#define MAX_CONSUMERS 2

#define CHECK_OR_RETURN(rv, cond, msg) \
  do { \
    if (!(cond)) { \
      fputs(msg, stderr); \
      fputs("\n", stderr); \
      return (rv); \
    } \
  } while (0)

#define CHECK_OR_GOTO_WITH_MSG(label, rv, val, msg, cond) \
  do { \
    if (!(cond)) { \
      if ((msg)) { \
        fputs((msg), stderr); \
        fputs("\n", stderr); \
      } \
      (rv) = (val); \
      goto label; \
    } \
  } while (0)

#define CHECK_OR_GOTO(label, rv, val, cond) \
  CHECK_OR_GOTO_WITH_MSG(label, rv, val, NULL, cond)

int init_producer(struct producer **producer,
                  struct producer *(*fn)(const char*),
                  const char *arg) {
  CHECK_OR_RETURN(0, *producer == 0, "there can only be one producer");

  *producer = fn(arg);
  CHECK_OR_RETURN(0, *producer, "failed to construct producer");

  return 1;
}

int add_consumer(struct consumer **consumers,
                 size_t *num_consumers,
                 struct consumer *(*fn)(const char*),
                 const char *arg) {
  CHECK_OR_RETURN(0, *num_consumers != MAX_CONSUMERS, "too many consumers");

  consumers[(*num_consumers)++] = fn(arg);
  CHECK_OR_RETURN(0, consumers[*num_consumers-1],
                  "failed to construct consumer");
  return 1;
}

int main(int argc, char *argv[]) {
  int rv = 0;

  struct producer *producer = NULL;
  struct consumer *consumers[MAX_CONSUMERS] = {0};
  size_t num_consumers = 0;

  size_t bufsize = DEFAULT_BUFFER_SIZE;
  char *buffer = NULL;

  for (int opt; (opt = getopt(argc, argv, "b:i:o:r:s")) != -1;) {
    switch (opt) {
    case 'b':
      bufsize = strtoll(optarg, NULL, 10);
      // TODO: check!
      break;
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

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "please specify a producer", producer);
  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "please specify at least one consumer",
                         num_consumers > 0);

  buffer = malloc(bufsize);

  CHECK_OR_GOTO_WITH_MSG(cleanup, rv, 1, "can't allocate memory for buffer",
                         buffer);

cleanup:
  if (producer) {
    // producer->destroy(producer);
    free(producer);
  }

  for (size_t i = MAX_CONSUMERS; i--;)
    if (consumers[i]) {
      // consumers[i]->destroy(consumers[i]);
      free(consumers[i]);
    }
  free(buffer);

  return rv;
}
