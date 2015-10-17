#pragma once

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


#define DO_RETURN(rv) return (rv)
#define DO_GOTO(label, retval, value) \
  do { \
    (retval) = (value); \
    goto label; \
  } while (0)

#define PERROR1(msg, arg) \
  do { \
    fputs(msg "", stderr); \
    perror(arg); \
  } while (0)

#define SYSCALL(expr) ((expr) != -1)

#define CHECK(cond, warn, act) \
  do { \
    if (!(cond)) {\
      warn; \
      act; \
    } \
  } while (0)

#define CHECK_SYSCALL_OR_RETURN(rv, res, msg, arg) \
  CHECK(SYSCALL(res), PERROR1(msg, arg), DO_RETURN(rv))

#define CHECK_SYSCALL_OR_GOTO(label, retval, value, call, msg, arg) \
  do { \
    if ((call) == -1) { \
      fputs(msg, stderr); \
      perror(arg); \
      (retval) = (value); \
      goto label; \
    } \
  } while (0)


#define CHECK_SYSCALL_OR_WARN(call, msg, arg) \
  do { \
    if ((call) == -1) { \
      fputs(msg, stderr); \
      perror(arg); \
    } \
  } while (0)

#define COND_CHECK_SYSCALL_OR_WARN(var, value, call, msg, arg) \
  do { \
    if (var != value) { \
      if ((call) == -1) { \
        fputs(msg, stderr); \
        perror(arg); \
      } \
    } \
    var = value; \
  } while (0)

#define CHECK_OR_RETURN(rv, cond, msg) \
  do { \
    if (!(cond)) { \
      fputs(msg, stderr); \
      fputs("\n", stderr); \
      return (rv); \
    } \
  } while (0)

#define CHECK_OR_GOTO_WITH_MSG(label, rv, val, msg, cond) \
  do { \
    if (!(cond)) { \
      if ((msg)) { \
        fputs((msg), stderr); \
        fputs("\n", stderr); \
      } \
      (rv) = (val); \
      goto label; \
    } \
  } while (0)

#define CHECK_OR_GOTO(label, rv, val, cond) \
  CHECK_OR_GOTO_WITH_MSG(label, rv, val, NULL, cond)


#define arraysize(arr) (sizeof(arr) / sizeof(arr[0]))
