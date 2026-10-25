CC = cc
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic

all: server client

server: server.c
	$(CC) $(CFLAGS) -pthread server.c -o server

client: client.c
	$(CC) $(CFLAGS) client.c -o client

clean:
	rm -f server client

.PHONY: all clean
