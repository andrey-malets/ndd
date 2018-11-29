#include "macro.h"
#include "stats.h"
#include "struct.h"

#include <assert.h>
#include <stdio.h>

bool dump_stats(struct state *state, const char *filename) {
  assert(state);
  assert(state->stats);

  bool rv = true;
  FILE *output = NULL;
  CHECK(output = fopen(filename, "w"),
        PERROR1("fopen() failed for", filename), GOTO_WITH(cleanup, rv, false));

#define PUT(string) \
  CHECK(fputs(string, output) != EOF, \
        perror("failed fo format output"), GOTO_WITH(cleanup, rv, false))
#define DUMP_VALUE(name, value, suffix) \
  CHECK(fprintf(output, "\"%s\": %"PRIu64"%s", \
                name, value, suffix) > 0, \
        PERROR1("failed to dump", name), GOTO_WITH(cleanup, rv, false))
#define DUMP_SIMPLE_VALUE(name, suffix) \
  DUMP_VALUE(# name, state->stats->name, suffix)

  PUT("{");

  {
    DUMP_SIMPLE_VALUE(total_cycles, ",");
    DUMP_SIMPLE_VALUE(waited_cycles, ",");
    DUMP_SIMPLE_VALUE(buffer_underruns, ",");
    DUMP_SIMPLE_VALUE(buffer_overruns, ",");

    PUT("\"consumer_slowdowns\": {");
    for (size_t i = 0; i != state->num_consumers; ++i)
      DUMP_VALUE(CALL0(state->consumers[i], name),
                 state->stats->consumer_slowdowns[i],
                 i == state->num_consumers - 1 ? "" : ",");
    PUT("}");
  }

  PUT("}\n");

#undef DUMP_VALUE
#undef PUT

cleanup:
  if (output)
    CHECK(fclose(output) == 0, PERROR1("fclose() failed for", filename),
          rv = false);

  return rv;
}
