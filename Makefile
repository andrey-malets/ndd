CC=clang
CFLAGS=-pedantic -Werror -Wall -std=c11

ndd: main.o file.o socket.o
	$(CC) -o $@ $^

main.o: main.c
file.o: file.c
socket.o: socket.c

clean:
	rm -f *.o ndd
