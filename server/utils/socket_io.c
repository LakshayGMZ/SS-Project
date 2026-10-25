#define _POSIX_C_SOURCE 200809L
#include "socket_io.h"

#include <errno.h>
#include <sys/socket.h>
#include <string.h>

ssize_t socket_receive_request(int socket_fd, char *buffer, size_t capacity) {
    ssize_t received;
    if (!buffer || capacity < 2) {
        errno = EINVAL;
        return -1;
    }
    do {
        received = recv(socket_fd, buffer, capacity - 1, 0);
    } while (received < 0 && errno == EINTR);
    if (received > 0) {
        buffer[received] = '\0';
        buffer[strcspn(buffer, "\r\n")] = '\0';
    }
    return received;
}

int socket_send_all(int socket_fd, const char *message) {
    size_t length = strlen(message);
    size_t sent_total = 0;
    while (sent_total < length) {
        ssize_t sent = send(socket_fd, message + sent_total, length - sent_total, 0);
        if (sent < 0 && errno == EINTR) continue;
        if (sent <= 0) return -1;
        sent_total += (size_t)sent;
    }
    return 0;
}
