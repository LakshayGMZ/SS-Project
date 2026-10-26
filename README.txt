BANKING MANAGEMENT SYSTEM (COLLEGE PROJECT)

BUILD
    make

This builds bank-server and client with the system C compiler, pthreads, and POSIX APIs. No external package is required.

RUN
    ./bank-server
    ./client

The server defaults to port 8080 and database directory ./database. Optional arguments are:
    ./bank-server [port] [database-directory]
    ./client [port]
The client defaults to 127.0.0.1:8080; pass a port to connect to 127.0.0.1 on another server port. Change SERVER_IP in client.c to use another host.

DEMO ACCOUNTS
    Administrator: admin / admin123
    Manager:       manager / manager123
    Employee:      employee / employee123
    Customer:      customer / customer123

SERVER STRUCTURE
    server/app/main.c                         socket listener and client workers
    server/components/request_handler.c       protocol, authorization, banking operations
    server/components/session_manager.c       shared text-file sessions and per-user locks
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

The server also creates a sessions/ directory containing one active-session text file per user, and a .locks/ directory containing POSIX advisory lock files for session ownership, startup recovery, WAL commits, and append-ID allocation. Multiple server processes can share a database directory on a filesystem that correctly supports POSIX fcntl record locks. Keep the database directory private to the server account.
