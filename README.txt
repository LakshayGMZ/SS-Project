BANKING MANAGEMENT SYSTEM (COLLEGE PROJECT)

BUILD
    make

This builds bank-server and client with the system C compiler, pthreads, and POSIX APIs. No external package is required.

RUN
    ./bank-server
    ./client

The server defaults to port 8080 and database directory ./database. Optional arguments are:
    ./bank-server [port] [database-directory]
The supplied client connects to 127.0.0.1:8080. Change SERVER_IP and PORT in client.c to use another endpoint.

DEMO ACCOUNTS
    Administrator: admin / admin123
    Manager:       manager / manager123
    Employee:      employee / employee123
    Customer:      customer / customer123

SERVER STRUCTURE
    server/app/main.c                         socket listener and client workers
    server/components/request_handler.c       protocol, authorization, banking operations
    server/components/session_manager.c       in-memory login sessions
    server/storage/database.c                text tables, record locks, append allocation, WAL
    server/utils/socket_io.c                 request receive and response send helpers
    server/utils/common.h                    shared role and data-record definitions
    server/*/*.h                             module interfaces

TEXT DATABASE
The database directory contains one fixed-width, pipe-delimited text file per table:
    users.txt
    accounts.txt
    loans.txt
    transactions.txt
    feedback.txt
