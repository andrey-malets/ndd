#pragma once

#include "macro.h"

#include <stdbool.h>

struct producer_ops {
  METHOD0(int, get_fd);
  METHOD0(bool, init);
  METHOD0(void, destroy);
};

struct producer {
  OBJECT(struct producer_ops);
};

struct consumer_ops {
  METHOD0(int, get_fd);
  METHOD0(bool, init);
  METHOD0(void, destroy);
};

struct consumer {
  OBJECT(struct consumer_ops);
};

bool is_empty_producer(struct producer *producer);
bool is_empty_consumer(struct consumer *consumer);
