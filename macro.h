#pragma once

#define METHOD0(rv, name) rv (*name)(void *ptr)
#define METHOD(rv, name, ...) rv (*name)(void *ptr, __VA_ARGS__)

#define OBJECT(ops_type) \
  const ops_type *ops; \
  void *data

#define CALL0(object, method) object.ops->method(object.data)
#define CALL(object, method, ...) object.ops->method(object.data, __VA_ARGS__)

#define GET(type, obj, ptr) type *obj = (type *)(ptr)
