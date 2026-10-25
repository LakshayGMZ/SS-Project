BANKING MANAGEMENT SYSTEM (COLLEGE PROJECT)

OVERVIEW
A small C client/server banking simulation. The client sends one pipe-delimited request over TCP for each menu action. The server uses POSIX sockets, pthreads, a process mutex, and fcntl advisory file locks. Records are stored in a human-readable text database.

BUILD
    make

START THE SERVER
    ./server
    ./server 8080 bank.txt

Start the client in another terminal:
    ./client

The supplied client defaults to 127.0.0.1:8080. Edit SERVER_IP and PORT near the top of client.c to connect to another server.

DEMO ACCOUNTS
    Administrator: admin / admin123
    Manager:       manager / manager123
    Employee:      employee / employee123
    Customer:      customer / customer123

The ZIP includes bank.txt with the demo accounts and a customer account balance of 1000.00. If bank.txt is absent, the server creates it with the same demo data. The database and its .lock file are created in the server's current directory unless another database path is supplied.

FEATURES
- Customer registration/login, balance, deposit, withdrawal, transfer, loan application, password change, transaction history, feedback, logout.
- Employees can create customer accounts, reset customer passwords, view passbooks, view assigned loans, and approve/reject assigned applications.
- Managers can activate/deactivate customer accounts, assign pending loans, and review feedback.
- Administrators can add employees, reset user passwords, and change user roles.
- One active session per user in a running server. Idle sessions expire after 30 minutes; restarting the server clears sessions.
- Currency is stored as integer cents. Amounts accept at most two decimal places.

DATA AND SYNCHRONIZATION
The text database uses record types U (user), A (account), L (loan), T (transaction), and F (feedback). The server serializes its worker threads with a pthread mutex because POSIX fcntl locks are process-associated on common systems. It also holds an exclusive fcntl lock on bank.txt.lock while loading and updating the database, so separate server processes sharing this database cannot interleave updates. A change is written to bank.txt.tmp, fsynced, then renamed over the database while the lock is held. Transfers and approved loans update balances, statuses, and transaction records in one database snapshot.

PROTOCOL
Each TCP connection carries one request in this format:
    COMMAND|ROLE_ID|USER_ID|ARGUMENTS...
Responses are SUCCESS|message or ERROR|message. LOGIN is LOGIN|role|username|password. CREATE_CUSTOMER is CREATE_CUSTOMER|1|username|password. See command names in client.c and dispatch logic in server.c. The server also supports the existing client's response expectation for successful registration/login: SUCCESS|user_id|message and SUCCESS|user_id|message respectively.

CLASSROOM PROJECT LIMITATIONS
This is an educational simulation, not production banking software. Passwords are stored in plain text, the protocol is unencrypted, and the client-supplied role/user IDs are not protected by a cryptographic session token. Do not use real personal data, passwords, or money. A real system would require password hashing, TLS, stronger session authentication, audit controls, and extensive security review.
