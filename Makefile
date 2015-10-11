CC=clang
CFLAGS=-pedantic -Werror -Wall -std=c11

ndd: main.o file.o socket.o struct.o
	$(CC) -o $@ $^

main.o: main.c
file.o: file.c
socket.o: socket.c
struct.o: struct.c

clean:
	rm -f *.o ndd
