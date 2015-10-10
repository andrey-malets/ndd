#include "file.h"
#include "struct.h"

#include <stdlib.h>

struct producer *get_file_reader(const char *filename) {
  return malloc(sizeof(struct producer));
}

struct consumer *get_file_writer(const char *filename) {
  return malloc(sizeof(struct producer));
}
