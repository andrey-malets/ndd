#pragma once

#include <stdlib.h>

struct producer;
struct consumer;

extern struct producer get_pipe_reader(const char *filename);
extern struct consumer get_pipe_writer(const char *filename,
                                       size_t lo_watermark);
