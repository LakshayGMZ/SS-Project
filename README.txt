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

The server creates and seeds these files on first start if all tables are empty. The accounts table contains only actual accounts, stored contiguously; each row's user_id links it to a user and remains the account number accepted by the client. Staff who have never been customers have no account row; a customer changed to another role keeps their existing account inactive. Account lookups match the stored user_id rather than assuming it is the physical row number. On startup, legacy blank account slots are compacted out. Startup validation checks unique usernames, unique account owners, customer-account relationships, and user references in loans, transactions, and feedback. Every stored row uses full table and role names, and full account states and transaction kinds; there are no one-letter table tags or numeric role/boolean codes.

LOCKING AND COMMIT
Each table row has a stable byte range because records have fixed widths. Database reads acquire a shared fcntl lock and updates acquire an exclusive fcntl lock only on the row being accessed. The range begins at (record_id - 1) * record_size. A small set of hashed pthread mutexes prevents same-process threads from relying on process-associated fcntl behavior. Separate append locks under database/.locks allocate IDs safely; they are not used to lock ordinary record access.

A request that changes several records keeps those record locks until commit. The database writes a write-ahead journal under database/.locks, fsyncs it, updates the affected table rows with pwrite, fsyncs those files, and then clears the journal. Startup and later requests replay a complete journal if a process stopped during a multi-table commit. This supports atomic transfers, loan credits, and account creation without rewriting whole tables. Journal, startup, append, and commit lock files are control files, not additional tables.

FEATURES
- Customers can register/login, check balances, deposit, withdraw, transfer, apply for loans, change passwords, add feedback, view history, and log out.
- Employees can create customers, reset customer passwords, view passbooks, and process assigned loans.
- Managers can activate/deactivate accounts, assign pending loans, and review feedback.
- Administrators can add employees, reset passwords, and manage roles.
- Deposits, withdrawals, transfers, and loan credits store integer cents and write transaction history.
- One active session per user per server process. Sessions are bound to a persistent client connection and removed on logout or when the server detects that connection closing; idle sessions expire after 30 minutes.

LIMITATIONS
This is an educational simulation, not production banking software. Passwords are stored in plain text, the protocol is unencrypted, sessions are process-local, and the simple protocol does not use cryptographic session tokens. Table records have fixed capacities defined in server/storage/database.c. Do not use real credentials or financial data.
