#include "file.h"
#include "struct.h"

#include <stdlib.h>

struct producer get_file_reader(const char *filename) {
  return (struct producer) {0, 0};
}

struct consumer get_file_writer(const char *filename) {
  return (struct consumer) {0, 0};
}
