CC=clang
CFLAGS=-pedantic -Werror -Wall -std=c11 -D_GNU_SOURCE

ndd: main.o file.o socket.o stats.o struct.o engine.o
	$(CC) -o $@ $^

clean:
	rm -f *.o ndd
