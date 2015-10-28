#pragma once

#define DEFAULT_BUFFER_SIZE (16*1024*1024)
#define DEFAULT_BLOCK_SIZE (8*1024*1024)

#define MAX_CONSUMERS 2

#define DEFAULT_PORT "3634"
static const unsigned int CONNECT_BACKOFF[] = {0, 1, 3, 5};
