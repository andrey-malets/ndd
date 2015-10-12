CC=clang
CFLAGS=-pedantic -Werror -Wall -std=c11 -D_GNU_SOURCE

ndd: main.o file.o socket.o struct.o engine.o
	$(CC) -o $@ $^

main.o: main.c
file.o: file.c
socket.o: socket.c
struct.o: struct.c
engine.o: engine.c

clean:
	rm -f *.o ndd
