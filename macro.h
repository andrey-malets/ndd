#pragma once

#define arraysize(arr) (sizeof(arr) / sizeof(arr[0]))

#define METHOD0(rv, name) rv (*name)(void *ptr)
#define METHOD(rv, name, ...) rv (*name)(void *ptr, __VA_ARGS__)

#define OBJECT(ops_type) \
  const ops_type *ops; \
  void *data

#define CALL0(object, method) (object).ops->method((object).data)
#define CALL(object, method, ...) (object).ops->method((object).data, __VA_ARGS__)

#define GET(type, obj, ptr) \
  type *obj = (type *)(ptr); \
  assert(obj)


#define GOTO_WITH(label, retval, value) \
  do { \
    (retval) = (value); \
    goto label; \
  } while (0)

#define ERROR(msg) \
  fputs(msg "\n", stderr)

#define PERROR1(msg, arg) \
  do { \
    fputs(msg " ", stderr); \
    perror(arg); \
  } while (0)

#define GAI_PERROR1(msg, arg) \
  fprintf(stderr, "%s %s: %s", msg, arg, gai_strerror(errno))

#define SYSCALL(expr) ((expr) != -1)

#define CHECK(cond, alert, act) \
  do { \
    if (!(cond)) {\
      alert; \
      act; \
    } \
  } while (0)

#define COND_CHECK(var, value, cond, alert) \
  do { \
    if (var != value) \
      CHECK(cond, alert, ;); \
    var = value; \
  } while (0)
