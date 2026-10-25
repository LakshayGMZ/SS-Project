#define _POSIX_C_SOURCE 200809L
#include "socket_io.h"

#include <errno.h>
#include <stdio.h>
#include <sys/socket.h>
#include <string.h>

ssize_t socket_receive_request(int socket_fd, char *buffer, size_t capacity) {
    size_t received = 0;
    if (!buffer || capacity < 2) {
        errno = EINVAL;
        return -1;
    }
    while (received < capacity - 1) {
        char byte;
        ssize_t result = recv(socket_fd, &byte, 1, 0);
        if (result < 0 && errno == EINTR) continue;
        if (result < 0) return -1;
        if (result == 0) {
            if (received == 0) return 0;
            errno = ECONNRESET;
            return -1;
        }
        if (byte == '\n') {
            if (received > 0 && buffer[received - 1] == '\r') received--;
            buffer[received] = '\0';
            return (ssize_t)received;
        }
        buffer[received++] = byte;
    }
    errno = EMSGSIZE;
    return -1;
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

int socket_send_frame(int socket_fd, const char *message) {
    char header[32];
    int length = snprintf(header, sizeof(header), "%zu\n", strlen(message));
    if (length < 0 || (size_t)length >= sizeof(header)) return -1;
    if (socket_send_all(socket_fd, header) != 0) return -1;
    return socket_send_all(socket_fd, message);
}
