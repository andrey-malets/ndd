#include "stddef.h"
#include "struct.h"

bool is_empty_producer(struct producer *producer) {
  return producer->data == NULL && producer->ops == NULL;
}

bool is_empty_consumer(struct consumer *consumer) {
  return consumer->data == NULL && consumer->ops == NULL;
}
