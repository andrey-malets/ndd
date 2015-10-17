#pragma once

#include "stdbool.h"
#include "stddef.h"

struct state;
bool transfer(size_t buffer_size, size_t block_size,
              int sleep_us, struct state *const state);
