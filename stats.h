#pragma once

#include "defaults.h"
#include "stdbool.h"

#include <inttypes.h>

struct stats {
  uint64_t total_cycles;
  uint64_t waited_cycles;
  uint64_t buffer_underruns;
  uint64_t buffer_overruns;
  uint64_t consumer_slowdowns[MAX_CONSUMERS];
};

#define EMPTY_STATS {0, 0, 0, 0, {0}}

#define INC(stats, counter) \
  do \
    if (stats) \
      ++stats->counter; \
  while (0)

struct state;
bool dump_stats(struct state *state, const char *filename);
