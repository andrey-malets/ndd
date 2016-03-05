#pragma once

#include <stdbool.h>
#include <stdlib.h>

bool would_block(int rv);

size_t get_zero_lo_watermark(void *data);

ssize_t zero_consume_signal(void *data);
