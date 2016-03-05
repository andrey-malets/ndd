CC=clang

PLATFORM=amd64
OUTPUT.amd64=ndd
OUTPUT.i386=ndd32
CFLAGS.i386=-m32

BUILD=debug
CFLAGS.common  = -pedantic -Werror -Wall -std=c11 -D_GNU_SOURCE
CFLAGS.debug   = -g -O0
CFLAGS.release = -O2

CFLAGS=${CFLAGS.common} ${CFLAGS.${BUILD}} ${CFLAGS.${PLATFORM}}

${OUTPUT.${PLATFORM}}: main.o file.o pipe.o socket.o stats.o struct.o \
					   engine.o util.o
	$(CC) $(CFLAGS) -o $@ $^
ifeq ($(BUILD), release)
	strip $@
endif

.PHONY: clean
clean:
	rm -f *.o $(OUTPUT.$(PLATFORM))
