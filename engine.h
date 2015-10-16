#pragma once

#include "struct.h"

bool transfer(size_t buffer_size, size_t block_size,
              long sleep_us, struct state *const state);
