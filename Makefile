CC = cc
CFLAGS = -std=c11 -Wall -Wextra -Wpedantic
SERVER_SOURCES = server/app/main.c \
	server/components/request_handler.c \
	server/components/session_manager.c \
	server/storage/database.c \
	server/utils/socket_io.c
SERVER_HEADERS = server/components/request_handler.h \
	server/components/session_manager.h \
	server/storage/database.h \
	server/utils/common.h \
	server/utils/socket_io.h

all: bank-server client

bank-server: $(SERVER_SOURCES) $(SERVER_HEADERS)
	$(CC) $(CFLAGS) -pthread -Iserver/components -Iserver/storage -Iserver/utils $(SERVER_SOURCES) -o bank-server

server: bank-server

client: client.c
	$(CC) $(CFLAGS) client.c -o client

clean:
	rm -f bank-server client

.PHONY: all server client clean
