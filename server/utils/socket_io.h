#ifndef BANK_SOCKET_IO_H
#define BANK_SOCKET_IO_H

#include <stddef.h>
#include <sys/types.h>

ssize_t socket_receive_request(int socket_fd, char *buffer, size_t capacity);
int socket_send_all(int socket_fd, const char *message);
int socket_send_frame(int socket_fd, const char *message);

#endif
