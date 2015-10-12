#pragma once

#include "struct.h"

bool transfer(size_t buffer_size, size_t block_size,
              struct producer producer,
              struct consumer *consumers,
              size_t num_consumers);
