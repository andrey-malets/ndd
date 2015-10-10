#pragma once

#include <stdbool.h>

struct producer;
struct producer_ops {
  int (*get_fd)();
  bool (*init)(struct producer *);
  void (*destroy)(struct producer *);
};

struct producer {
  struct producer_ops *ops;
  void *data;
};

struct consumer;
struct consumer_ops {
  int (*get_fd)(struct consumer *);
  bool (*init)(struct consumer *);
  void (*destroy)(struct consumer *);
};

struct consumer {
  struct consumer_ops *ops;
  void *data;
};
