#define _POSIX_C_SOURCE 200809L
#include "../storage/database.h"
#include "../components/request_handler.h"
#include "../components/session_manager.h"
#include "../utils/socket_io.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

typedef struct {
    int socket_fd;
    Database *database;
} ClientConnection;


static void *client_worker(void *argument) {
    ClientConnection *connection = argument;
    char request[REQUEST_SIZE];
    char response[RESPONSE_SIZE];
    uintptr_t connection_id = (uintptr_t)connection;
    ssize_t received;
    while ((received = socket_receive_request(connection->socket_fd, request,
                                               sizeof(request))) > 0) {
        handle_request(connection->database, request, response, connection_id);
        if (socket_send_frame(connection->socket_fd, response) != 0) break;
    }
    sessions_logout_connection(connection_id);
    close(connection->socket_fd);
    free(connection);
    return NULL;
}

static int parse_port(const char *text, int *port) {
    char *end = NULL;
    long parsed = strtol(text, &end, 10);
    if (!text[0] || *end || parsed < 1 || parsed > 65535) return -1;
    *port = (int)parsed;
    return 0;
}

int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    int server_fd;
    int reuse = 1;
    struct sockaddr_in address;
    char error[256];
    const char *database_directory = "database";

    if (argc > 1 && parse_port(argv[1], &port) != 0) {
        fprintf(stderr, "Usage: %s [port] [database-directory]\n", argv[0]);
        return 1;
    }
    if (argc > 2) database_directory = argv[2];
    if (argc > 3) {
        fprintf(stderr, "Usage: %s [port] [database-directory]\n", argv[0]);
        return 1;
    }

    Database *database = db_open(database_directory, error, sizeof(error));
    if (!database) {
        fprintf(stderr, "Database error: %s\n", error);
        return 1;
    }
    sessions_init();
    signal(SIGPIPE, SIG_IGN);

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        perror("socket");
        db_close(database);
        return 1;
    }
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons((unsigned short)port);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0 ||
        listen(server_fd, 32) < 0) {
        perror("bind/listen");
        close(server_fd);
        db_close(database);
        return 1;
    }

    printf("Bank server listening on port %d; database directory: %s\n", port, database_directory);
    printf("Demo logins: admin/admin123, manager/manager123, employee/employee123, customer/customer123\n");
    for (;;) {
        ClientConnection *connection = malloc(sizeof(*connection));
        pthread_t thread;
        if (!connection) continue;
        connection->socket_fd = accept(server_fd, NULL, NULL);
        if (connection->socket_fd < 0) {
            free(connection);
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }
        connection->database = database;
        if (pthread_create(&thread, NULL, client_worker, connection) != 0) {
            close(connection->socket_fd);
            free(connection);
            continue;
        }
        pthread_detach(thread);
    }
}
