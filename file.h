#pragma once

#include <stdlib.h>

struct producer;
struct consumer;

extern struct producer get_file_reader(const char *filename);
extern struct consumer get_file_writer(const char *filename,
                                       size_t lo_watermark);
