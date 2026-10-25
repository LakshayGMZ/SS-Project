#define _POSIX_C_SOURCE 200809L
#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_PORT 8080
#define REQUEST_SIZE 2048
#define RESPONSE_SIZE 8192
#define MAX_USERS 2000
#define MAX_ACCOUNTS 2000
#define MAX_LOANS 4000
#define MAX_TRANSACTIONS 20000
#define MAX_FEEDBACK 4000
#define MAX_SESSIONS 2000
#define SESSION_TIMEOUT 1800
#define USERNAME_SIZE 50
#define PASSWORD_SIZE 50
#define TEXT_SIZE 256

enum Role { CUSTOMER = 1, EMPLOYEE = 2, MANAGER = 3, ADMIN = 4 };

typedef struct {
    int id;
    int role;
    char username[USERNAME_SIZE];
    char password[PASSWORD_SIZE];
} User;

typedef struct {
    int user_id;
    long long balance;
    bool active;
} Account;

typedef struct {
    int id;
    int customer_id;
    long long amount;
    char status[16];
    int employee_id;
} Loan;

typedef struct {
    int id;
    int customer_id;
    long long amount;
    char type[20];
    char details[TEXT_SIZE];
    char timestamp[32];
} Transaction;

typedef struct {
    int id;
    int customer_id;
    char text[TEXT_SIZE];
} Feedback;

typedef struct {
    User users[MAX_USERS];
    Account accounts[MAX_ACCOUNTS];
    Loan loans[MAX_LOANS];
    Transaction transactions[MAX_TRANSACTIONS];
    Feedback feedback[MAX_FEEDBACK];
    size_t user_count;
    size_t account_count;
    size_t loan_count;
    size_t transaction_count;
    size_t feedback_count;
} State;

typedef struct {
    int user_id;
    int role;
    time_t last_seen;
    bool active;
} Session;

static char database_path[PATH_MAX];
static char lock_path[PATH_MAX];
static char temporary_path[PATH_MAX];
static Session sessions[MAX_SESSIONS];
static pthread_mutex_t process_mutex = PTHREAD_MUTEX_INITIALIZER;

static void reply(char *out, const char *status, const char *message) {
    snprintf(out, RESPONSE_SIZE, "%s|%s", status, message ? message : "");
}

static bool safe_field(const char *value) {
    return value && !strchr(value, '|') && !strchr(value, '\n') && !strchr(value, '\r');
}

static bool parse_id(const char *value, int *id) {
    char *end = NULL;
    long parsed;
    if (!value || !*value) return false;
    errno = 0;
    parsed = strtol(value, &end, 10);
    if (errno || *end || parsed < 1 || parsed > INT_MAX) return false;
    *id = (int)parsed;
    return true;
}

static bool parse_money(const char *value, long long *amount, bool allow_zero) {
    unsigned long long whole = 0;
    unsigned int fraction = 0;
    int fraction_digits = 0;
    bool saw_digit = false;
    const char *cursor = value;
    if (!value || !*value) return false;
    while (*cursor >= '0' && *cursor <= '9') {
        unsigned int digit = (unsigned int)(*cursor - '0');
        if (whole > ((unsigned long long)LLONG_MAX / 100ULL - digit) / 10ULL) return false;
        whole = whole * 10ULL + digit;
        saw_digit = true;
        cursor++;
    }
    if (*cursor == '.') {
        cursor++;
        while (*cursor >= '0' && *cursor <= '9' && fraction_digits < 2) {
            fraction = fraction * 10U + (unsigned int)(*cursor - '0');
            fraction_digits++;
            cursor++;
        }
        if (fraction_digits == 0 || (*cursor >= '0' && *cursor <= '9')) return false;
    }
    if (!saw_digit || *cursor != '\0') return false;
    if (fraction_digits == 1) fraction *= 10U;
    if (whole > (unsigned long long)LLONG_MAX / 100ULL) return false;
    unsigned long long cents = whole * 100ULL + fraction;
    if (cents > (unsigned long long)LLONG_MAX || (!allow_zero && cents == 0)) return false;
    *amount = (long long)cents;
    return true;
}

static bool parse_amount(const char *value, long long *amount) {
    return parse_money(value, amount, false);
}

static bool parse_balance(const char *value, long long *amount) {
    return parse_money(value, amount, true);
}

static void format_amount(long long cents, char *out, size_t size) {
    snprintf(out, size, "%lld.%02lld", cents / 100, llabs(cents % 100));
}

static User *find_user(State *state, int id) {
    for (size_t i = 0; i < state->user_count; i++)
        if (state->users[i].id == id) return &state->users[i];
    return NULL;
}

static User *find_username(State *state, const char *username) {
    for (size_t i = 0; i < state->user_count; i++)
        if (strcmp(state->users[i].username, username) == 0) return &state->users[i];
    return NULL;
}

static Account *find_account(State *state, int user_id) {
    for (size_t i = 0; i < state->account_count; i++)
        if (state->accounts[i].user_id == user_id) return &state->accounts[i];
    return NULL;
}

static Loan *find_loan(State *state, int id) {
    for (size_t i = 0; i < state->loan_count; i++)
        if (state->loans[i].id == id) return &state->loans[i];
    return NULL;
}

static int next_user_id(State *state) {
    int max_id = 0;
    for (size_t i = 0; i < state->user_count; i++)
        if (state->users[i].id > max_id) max_id = state->users[i].id;
    return max_id + 1;
}

static int next_loan_id(State *state) {
    int max_id = 0;
    for (size_t i = 0; i < state->loan_count; i++)
        if (state->loans[i].id > max_id) max_id = state->loans[i].id;
    return max_id + 1;
}

static int next_transaction_id(State *state) {
    int max_id = 0;
    for (size_t i = 0; i < state->transaction_count; i++)
        if (state->transactions[i].id > max_id) max_id = state->transactions[i].id;
    return max_id + 1;
}

static int next_feedback_id(State *state) {
    int max_id = 0;
    for (size_t i = 0; i < state->feedback_count; i++)
        if (state->feedback[i].id > max_id) max_id = state->feedback[i].id;
    return max_id + 1;
}

static void add_transaction(State *state, int customer_id, const char *type,
                            long long amount, const char *details) {
    Transaction *transaction;
    if (state->transaction_count >= MAX_TRANSACTIONS) return;
    int id = next_transaction_id(state);
    transaction = &state->transactions[state->transaction_count++];
    memset(transaction, 0, sizeof(*transaction));
    transaction->id = id;
    transaction->customer_id = customer_id;
    transaction->amount = amount;
    snprintf(transaction->type, sizeof(transaction->type), "%s", type);
    snprintf(transaction->details, sizeof(transaction->details), "%s", details);
    time_t now = time(NULL);
    struct tm time_parts;
    localtime_r(&now, &time_parts);
    strftime(transaction->timestamp, sizeof(transaction->timestamp), "%Y-%m-%d %H:%M:%S", &time_parts);
}

static void add_user(State *state, int id, int role, const char *username, const char *password) {
    User *user = &state->users[state->user_count++];
    user->id = id;
    user->role = role;
    snprintf(user->username, sizeof(user->username), "%s", username);
    snprintf(user->password, sizeof(user->password), "%s", password);
}

static void seed_state(State *state) {
    memset(state, 0, sizeof(*state));
    add_user(state, 1, ADMIN, "admin", "admin123");
    add_user(state, 2, MANAGER, "manager", "manager123");
    add_user(state, 3, EMPLOYEE, "employee", "employee123");
    add_user(state, 4, CUSTOMER, "customer", "customer123");
    state->accounts[0] = (Account){ .user_id = 4, .balance = 100000, .active = true };
    state->account_count = 1;
    add_transaction(state, 4, "OPENING", 100000, "Initial demo balance");
}

static int load_state(State *state) {
    FILE *file = fopen(database_path, "r");
    char *line = NULL;
    size_t capacity = 0;
    ssize_t length;
    if (!file) {
        if (errno == ENOENT) {
            seed_state(state);
            return 1;
        }
        return -1;
    }

    memset(state, 0, sizeof(*state));
    while ((length = getline(&line, &capacity, file)) >= 0) {
        char *save = NULL;
        char *kind;
        (void)length;
        line[strcspn(line, "\r\n")] = '\0';
        kind = strtok_r(line, "|", &save);
        if (!kind) continue;
        if (strcmp(kind, "U") == 0 && state->user_count < MAX_USERS) {
            char *id = strtok_r(NULL, "|", &save), *role = strtok_r(NULL, "|", &save);
            char *username = strtok_r(NULL, "|", &save), *password = strtok_r(NULL, "|", &save);
            if (id && role && username && password) {
                User *u = &state->users[state->user_count++];
                u->id = atoi(id); u->role = atoi(role);
                snprintf(u->username, sizeof(u->username), "%s", username);
                snprintf(u->password, sizeof(u->password), "%s", password);
            }
        } else if (strcmp(kind, "A") == 0 && state->account_count < MAX_ACCOUNTS) {
            char *id = strtok_r(NULL, "|", &save), *balance = strtok_r(NULL, "|", &save);
            char *active = strtok_r(NULL, "|", &save);
            if (id && balance && active)
                state->accounts[state->account_count++] = (Account){ atoi(id), atoll(balance), atoi(active) != 0 };
        } else if (strcmp(kind, "L") == 0 && state->loan_count < MAX_LOANS) {
            char *id = strtok_r(NULL, "|", &save), *customer = strtok_r(NULL, "|", &save);
            char *amount = strtok_r(NULL, "|", &save), *status = strtok_r(NULL, "|", &save);
            char *employee = strtok_r(NULL, "|", &save);
            if (id && customer && amount && status && employee) {
                Loan *loan = &state->loans[state->loan_count++];
                loan->id = atoi(id); loan->customer_id = atoi(customer); loan->amount = atoll(amount);
                loan->employee_id = atoi(employee); snprintf(loan->status, sizeof(loan->status), "%s", status);
            }
        } else if (strcmp(kind, "T") == 0 && state->transaction_count < MAX_TRANSACTIONS) {
            char *id = strtok_r(NULL, "|", &save), *customer = strtok_r(NULL, "|", &save);
            char *amount = strtok_r(NULL, "|", &save), *type = strtok_r(NULL, "|", &save);
            char *timestamp = strtok_r(NULL, "|", &save), *details = strtok_r(NULL, "|", &save);
            if (id && customer && amount && type && timestamp && details) {
                Transaction *t = &state->transactions[state->transaction_count++];
                t->id = atoi(id); t->customer_id = atoi(customer); t->amount = atoll(amount);
                snprintf(t->type, sizeof(t->type), "%s", type);
                snprintf(t->timestamp, sizeof(t->timestamp), "%s", timestamp);
                snprintf(t->details, sizeof(t->details), "%s", details);
            }
        } else if (strcmp(kind, "F") == 0 && state->feedback_count < MAX_FEEDBACK) {
            char *id = strtok_r(NULL, "|", &save), *customer = strtok_r(NULL, "|", &save);
            char *text = strtok_r(NULL, "|", &save);
            if (id && customer && text) {
                Feedback *f = &state->feedback[state->feedback_count++];
                f->id = atoi(id); f->customer_id = atoi(customer);
                snprintf(f->text, sizeof(f->text), "%s", text);
            }
        }
    }
    free(line);
    if (ferror(file)) { fclose(file); return -1; }
    fclose(file);
    return 0;
}

static int save_state(const State *state) {
    int fd = open(temporary_path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) return -1;
    FILE *file = fdopen(fd, "w");
    if (!file) { close(fd); return -1; }
    for (size_t i = 0; i < state->user_count; i++)
        fprintf(file, "U|%d|%d|%s|%s\n", state->users[i].id, state->users[i].role,
                state->users[i].username, state->users[i].password);
    for (size_t i = 0; i < state->account_count; i++)
        fprintf(file, "A|%d|%lld|%d\n", state->accounts[i].user_id,
                state->accounts[i].balance, state->accounts[i].active ? 1 : 0);
    for (size_t i = 0; i < state->loan_count; i++)
        fprintf(file, "L|%d|%d|%lld|%s|%d\n", state->loans[i].id, state->loans[i].customer_id,
                state->loans[i].amount, state->loans[i].status, state->loans[i].employee_id);
    for (size_t i = 0; i < state->transaction_count; i++)
        fprintf(file, "T|%d|%d|%lld|%s|%s|%s\n", state->transactions[i].id,
                state->transactions[i].customer_id, state->transactions[i].amount,
                state->transactions[i].type, state->transactions[i].timestamp,
                state->transactions[i].details);
    for (size_t i = 0; i < state->feedback_count; i++)
        fprintf(file, "F|%d|%d|%s\n", state->feedback[i].id,
                state->feedback[i].customer_id, state->feedback[i].text);
    int write_failed = fflush(file) != 0;
    if (!write_failed && fsync(fd) != 0) write_failed = 1;
    if (fclose(file) != 0) write_failed = 1;
    if (write_failed) {
        unlink(temporary_path);
        return -1;
    }
    if (rename(temporary_path, database_path) != 0) {
        unlink(temporary_path);
        return -1;
    }
    return 0;
}

static int lock_database(void) {
    int fd = open(lock_path, O_RDWR | O_CREAT, 0600);
    struct flock lock = { .l_type = F_WRLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0 };
    if (fd < 0) return -1;
    while (fcntl(fd, F_SETLKW, &lock) < 0) {
        if (errno == EINTR) continue;
        close(fd);
        return -1;
    }
    return fd;
}

static void unlock_database(int fd) {
    struct flock lock = { .l_type = F_UNLCK, .l_whence = SEEK_SET, .l_start = 0, .l_len = 0 };
    fcntl(fd, F_SETLK, &lock);
    close(fd);
}

static void expire_sessions(time_t now) {
    for (size_t i = 0; i < MAX_SESSIONS; i++)
        if (sessions[i].active && now - sessions[i].last_seen > SESSION_TIMEOUT)
            sessions[i].active = false;
}

static Session *find_session(int user_id) {
    for (size_t i = 0; i < MAX_SESSIONS; i++)
        if (sessions[i].active && sessions[i].user_id == user_id) return &sessions[i];
    return NULL;
}

static bool start_session(int user_id, int role) {
    time_t now = time(NULL);
    expire_sessions(now);
    if (find_session(user_id)) return false;
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        if (!sessions[i].active) {
            sessions[i] = (Session){ user_id, role, now, true };
            return true;
        }
    }
    return false;
}

static bool authorize(State *state, int role, int user_id, char *out) {
    expire_sessions(time(NULL));
    Session *session = find_session(user_id);
    User *user = find_user(state, user_id);
    if (!session || session->role != role || !user || user->role != role) {
        reply(out, "ERROR", "Please log in again.");
        return false;
    }
    session->last_seen = time(NULL);
    return true;
}

static bool add_customer_account(State *state, int id, long long balance) {
    if (state->user_count >= MAX_USERS || state->account_count >= MAX_ACCOUNTS) return false;
    state->accounts[state->account_count++] = (Account){ id, balance, true };
    if (balance > 0) add_transaction(state, id, "OPENING", balance, "Initial account balance");
    return true;
}

static bool append_text(char *out, size_t size, size_t *used, const char *text) {
    int written = snprintf(out + *used, size - *used, "%s", text);
    if (written < 0 || (size_t)written >= size - *used) return false;
    *used += (size_t)written;
    return true;
}

static void handle_request(State *state, char *request, char *out, bool *changed) {
    char *fields[8] = {0};
    char *save = NULL;
    size_t count = 0;
    for (char *part = strtok_r(request, "|", &save); part && count < 8;
         part = strtok_r(NULL, "|", &save)) fields[count++] = part;
    *changed = false;
    if (count == 0) { reply(out, "ERROR", "Empty request."); return; }

    if (strcmp(fields[0], "CREATE_CUSTOMER") == 0) {
        int role;
        if (count != 4 || !parse_id(fields[1], &role) || role != CUSTOMER ||
            !safe_field(fields[2]) || !safe_field(fields[3]) || !*fields[2] || !*fields[3] ||
            strlen(fields[2]) >= USERNAME_SIZE || strlen(fields[3]) >= PASSWORD_SIZE) {
            reply(out, "ERROR", "Invalid registration details."); return;
        }
        if (find_username(state, fields[2])) { reply(out, "ERROR", "Username already exists."); return; }
        if (state->user_count >= MAX_USERS || state->account_count >= MAX_ACCOUNTS) {
            reply(out, "ERROR", "Database capacity reached."); return;
        }
        int id = next_user_id(state);
        add_user(state, id, CUSTOMER, fields[2], fields[3]);
        add_customer_account(state, id, 0);
        if (!start_session(id, CUSTOMER)) { reply(out, "ERROR", "Could not create session."); return; }
        snprintf(out, RESPONSE_SIZE, "SUCCESS|%d|Account created.", id);
        *changed = true;
        return;
    }

    if (strcmp(fields[0], "LOGIN") == 0) {
        int role;
        if (count != 4 || !parse_id(fields[1], &role) || role < CUSTOMER || role > ADMIN) {
            reply(out, "ERROR", "Invalid login request."); return;
        }
        User *user = find_username(state, fields[2]);
        if (!user || user->role != role || strcmp(user->password, fields[3]) != 0) {
            reply(out, "ERROR", "Invalid username, password, or role."); return;
        }
        Account *account = role == CUSTOMER ? find_account(state, user->id) : NULL;
        if (role == CUSTOMER && (!account || !account->active)) {
            reply(out, "ERROR", "Customer account is inactive."); return;
        }
        if (!start_session(user->id, role)) { reply(out, "ERROR", "This user already has an active session."); return; }
        snprintf(out, RESPONSE_SIZE, "SUCCESS|%d|Login successful.", user->id);
        return;
    }

    int role, user_id;
    if (count < 3 || !parse_id(fields[1], &role) || !parse_id(fields[2], &user_id) ||
        role < CUSTOMER || role > ADMIN || !authorize(state, role, user_id, out)) return;

    if (strcmp(fields[0], "LOGOUT") == 0) {
        Session *session = find_session(user_id);
        if (session) session->active = false;
        reply(out, "SUCCESS", "Logged out."); return;
    }

    if (strcmp(fields[0], "CHANGE_PASS") == 0) {
        if (count != 4 || !safe_field(fields[3]) || !*fields[3] || strlen(fields[3]) >= PASSWORD_SIZE) {
            reply(out, "ERROR", "Password must be 1 to 49 characters and cannot contain '|'."); return;
        }
        User *user = find_user(state, user_id);
        snprintf(user->password, sizeof(user->password), "%s", fields[3]);
        *changed = true; reply(out, "SUCCESS", "Password changed."); return;
    }

    if (strcmp(fields[0], "VIEW_BALANCE") == 0 && role == CUSTOMER) {
        Account *account = find_account(state, user_id);
        char amount[64];
        if (!account || !account->active) { reply(out, "ERROR", "Account is inactive."); return; }
        format_amount(account->balance, amount, sizeof(amount));
        snprintf(out, RESPONSE_SIZE, "SUCCESS|Available balance: %s", amount); return;
    }

    if ((strcmp(fields[0], "DEPOSIT") == 0 || strcmp(fields[0], "WITHDRAW") == 0) && role == CUSTOMER) {
        long long amount;
        Account *account = find_account(state, user_id);
        bool deposit = strcmp(fields[0], "DEPOSIT") == 0;
        if (count != 4 || !parse_amount(fields[3], &amount)) { reply(out, "ERROR", "Enter a valid positive amount."); return; }
        if (!account || !account->active) { reply(out, "ERROR", "Account is inactive."); return; }
        if (state->transaction_count >= MAX_TRANSACTIONS) { reply(out, "ERROR", "Transaction capacity reached."); return; }
        if (!deposit && amount > account->balance) { reply(out, "ERROR", "Insufficient funds."); return; }
        if (deposit && account->balance > LLONG_MAX - amount) { reply(out, "ERROR", "Balance limit exceeded."); return; }
        account->balance += deposit ? amount : -amount;
        char amount_text[64]; format_amount(amount, amount_text, sizeof(amount_text));
        add_transaction(state, user_id, deposit ? "DEPOSIT" : "WITHDRAW", amount,
                        deposit ? "Cash deposit" : "Cash withdrawal");
        *changed = true;
        snprintf(out, RESPONSE_SIZE, "SUCCESS|%s of %s completed. New balance: %lld.%02lld",
                 deposit ? "Deposit" : "Withdrawal", amount_text,
                 account->balance / 100, llabs(account->balance % 100));
        return;
    }

    if (strcmp(fields[0], "TRANSFER") == 0 && role == CUSTOMER) {
        int target_id;
        long long amount;
        Account *source = find_account(state, user_id);
        if (count != 5 || !parse_id(fields[3], &target_id) || !parse_amount(fields[4], &amount)) {
            reply(out, "ERROR", "Enter a valid target account and positive amount."); return;
        }
        Account *target = find_account(state, target_id);
        if (!source || !source->active || !target || !target->active || !find_user(state, target_id) ||
            find_user(state, target_id)->role != CUSTOMER) {
            reply(out, "ERROR", "Target customer account is unavailable."); return;
        }
        if (target_id == user_id) { reply(out, "ERROR", "Cannot transfer to the same account."); return; }
        if (source->balance < amount) { reply(out, "ERROR", "Insufficient funds."); return; }
        if (target->balance > LLONG_MAX - amount) { reply(out, "ERROR", "Target balance limit exceeded."); return; }
        if (state->transaction_count > MAX_TRANSACTIONS - 2) { reply(out, "ERROR", "Transaction capacity reached."); return; }
        source->balance -= amount; target->balance += amount;
        char details[TEXT_SIZE];
        snprintf(details, sizeof(details), "Transfer to customer %d", target_id);
        add_transaction(state, user_id, "TRANSFER_OUT", amount, details);
        snprintf(details, sizeof(details), "Transfer from customer %d", user_id);
        add_transaction(state, target_id, "TRANSFER_IN", amount, details);
        char amount_text[64]; format_amount(amount, amount_text, sizeof(amount_text));
        snprintf(out, RESPONSE_SIZE, "SUCCESS|Transfer of %s to account %d completed.", amount_text, target_id);
        *changed = true; return;
    }

    if (strcmp(fields[0], "APPLY_LOAN") == 0 && role == CUSTOMER) {
        long long amount;
        Account *account = find_account(state, user_id);
        if (count != 4 || !parse_amount(fields[3], &amount)) { reply(out, "ERROR", "Enter a valid positive loan amount."); return; }
        if (!account || !account->active) { reply(out, "ERROR", "Account is inactive."); return; }
        if (state->loan_count >= MAX_LOANS) { reply(out, "ERROR", "Loan capacity reached."); return; }
        int loan_id = next_loan_id(state);
        Loan *loan = &state->loans[state->loan_count++];
        *loan = (Loan){ .id = loan_id, .customer_id = user_id,
                        .amount = amount, .employee_id = 0 };
        snprintf(loan->status, sizeof(loan->status), "PENDING");
        *changed = true;
        snprintf(out, RESPONSE_SIZE, "SUCCESS|Loan application submitted. Loan ID: %d", loan->id);
        return;
    }

    if (strcmp(fields[0], "VIEW_HISTORY") == 0 && role == CUSTOMER) {
        size_t used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        for (size_t i = 0; i < state->transaction_count; i++) {
            Transaction *t = &state->transactions[i];
            if (t->customer_id != user_id) continue;
            char amount[64], row[512];
            format_amount(t->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "%s | %s | %s | %s\n", t->timestamp, t->type, amount, t->details);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No transactions yet.");
        return;
    }

    if (strcmp(fields[0], "ADD_FEEDBACK") == 0 && role == CUSTOMER) {
        if (count != 4 || !safe_field(fields[3]) || !*fields[3] || strlen(fields[3]) >= TEXT_SIZE) {
            reply(out, "ERROR", "Feedback must be 1 to 255 characters and cannot contain '|'."); return;
        }
        if (state->feedback_count >= MAX_FEEDBACK) { reply(out, "ERROR", "Feedback capacity reached."); return; }
        int feedback_id = next_feedback_id(state);
        Feedback *feedback = &state->feedback[state->feedback_count++];
        feedback->id = feedback_id; feedback->customer_id = user_id;
        snprintf(feedback->text, sizeof(feedback->text), "%s", fields[3]);
        *changed = true; reply(out, "SUCCESS", "Feedback submitted."); return;
    }

    if (strcmp(fields[0], "ADD_CUSTOMER") == 0 && role == EMPLOYEE) {
        long long balance;
        if (count != 6 || !safe_field(fields[3]) || !safe_field(fields[4]) || !*fields[3] || !*fields[4] ||
            strlen(fields[3]) >= USERNAME_SIZE || strlen(fields[4]) >= PASSWORD_SIZE ||
            !parse_balance(fields[5], &balance)) {
            reply(out, "ERROR", "Invalid customer details or opening balance."); return;
        }
        if (find_username(state, fields[3])) { reply(out, "ERROR", "Username already exists."); return; }
        if (state->user_count >= MAX_USERS || state->account_count >= MAX_ACCOUNTS ||
            (balance > 0 && state->transaction_count >= MAX_TRANSACTIONS)) {
            reply(out, "ERROR", "Database capacity reached."); return;
        }
        int id = next_user_id(state);
        add_user(state, id, CUSTOMER, fields[3], fields[4]);
        add_customer_account(state, id, balance);
        *changed = true; snprintf(out, RESPONSE_SIZE, "SUCCESS|Customer created. User ID: %d", id); return;
    }

    if (strcmp(fields[0], "MOD_CUSTOMER") == 0 && role == EMPLOYEE) {
        int target_id;
        if (count != 5 || !parse_id(fields[3], &target_id) || !safe_field(fields[4]) || !*fields[4] ||
            strlen(fields[4]) >= PASSWORD_SIZE) { reply(out, "ERROR", "Invalid customer ID or password."); return; }
        User *target = find_user(state, target_id);
        if (!target || target->role != CUSTOMER) { reply(out, "ERROR", "Customer not found."); return; }
        snprintf(target->password, sizeof(target->password), "%s", fields[4]);
        *changed = true; reply(out, "SUCCESS", "Customer password updated."); return;
    }

    if (strcmp(fields[0], "VIEW_ASSIGNED_LOANS") == 0 && role == EMPLOYEE) {
        size_t used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        for (size_t i = 0; i < state->loan_count; i++) {
            Loan *loan = &state->loans[i];
            if (loan->employee_id != user_id || strcmp(loan->status, "ASSIGNED") != 0) continue;
            char amount[64], row[256]; format_amount(loan->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "Loan %d | Customer %d | Amount %s | %s\n",
                     loan->id, loan->customer_id, amount, loan->status);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No assigned loan applications.");
        return;
    }

    if (strcmp(fields[0], "PROCESS_LOAN") == 0 && role == EMPLOYEE) {
        int loan_id;
        if (count != 5 || !parse_id(fields[3], &loan_id) ||
            (strcmp(fields[4], "APPROVED") != 0 && strcmp(fields[4], "REJECTED") != 0)) {
            reply(out, "ERROR", "Loan ID or decision is invalid."); return;
        }
        Loan *loan = find_loan(state, loan_id);
        if (!loan || loan->employee_id != user_id || strcmp(loan->status, "ASSIGNED") != 0) {
            reply(out, "ERROR", "Loan is not assigned to this employee."); return;
        }
        Account *account = find_account(state, loan->customer_id);
        if (strcmp(fields[4], "APPROVED") == 0) {
            if (!account || !account->active) { reply(out, "ERROR", "Customer account is unavailable."); return; }
            if (account->balance > LLONG_MAX - loan->amount || state->transaction_count >= MAX_TRANSACTIONS) {
                reply(out, "ERROR", "Could not apply loan due to balance or database limit."); return;
            }
            account->balance += loan->amount;
            char details[TEXT_SIZE]; snprintf(details, sizeof(details), "Approved loan %d", loan_id);
            add_transaction(state, loan->customer_id, "LOAN_CREDIT", loan->amount, details);
        }
        snprintf(loan->status, sizeof(loan->status), "%s", fields[4]);
        *changed = true; snprintf(out, RESPONSE_SIZE, "SUCCESS|Loan %d %s.", loan_id,
                                   strcmp(fields[4], "APPROVED") == 0 ? "approved" : "rejected"); return;
    }

    if (strcmp(fields[0], "VIEW_PASSBOOK") == 0 && role == EMPLOYEE) {
        int customer_id;
        if (count != 4 || !parse_id(fields[3], &customer_id) || !find_account(state, customer_id)) {
            reply(out, "ERROR", "Customer account not found."); return;
        }
        size_t used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        for (size_t i = 0; i < state->transaction_count; i++) {
            Transaction *t = &state->transactions[i];
            if (t->customer_id != customer_id) continue;
            char amount[64], row[512]; format_amount(t->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "%s | %s | %s | %s\n", t->timestamp, t->type, amount, t->details);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No transactions yet.");
        return;
    }

    if (strcmp(fields[0], "ACT_DEACT_ACC") == 0 && role == MANAGER) {
        int target_id;
        if (count != 5 || !parse_id(fields[3], &target_id) ||
            (strcmp(fields[4], "ACTIVATE") != 0 && strcmp(fields[4], "DEACTIVATE") != 0)) {
            reply(out, "ERROR", "Customer ID or account status is invalid."); return;
        }
        Account *account = find_account(state, target_id);
        User *customer = find_user(state, target_id);
        if (!account || !customer || customer->role != CUSTOMER) { reply(out, "ERROR", "Customer account not found."); return; }
        account->active = strcmp(fields[4], "ACTIVATE") == 0;
        *changed = true; reply(out, "SUCCESS", account->active ? "Account activated." : "Account deactivated."); return;
    }

    if (strcmp(fields[0], "VIEW_LOANS") == 0 && role == MANAGER) {
        size_t used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        for (size_t i = 0; i < state->loan_count; i++) {
            Loan *loan = &state->loans[i];
            if (strcmp(loan->status, "PENDING") != 0) continue;
            char amount[64], row[256]; format_amount(loan->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "Loan %d | Customer %d | Amount %s | %s\n",
                     loan->id, loan->customer_id, amount, loan->status);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No pending loan applications.");
        return;
    }

    if (strcmp(fields[0], "ASSIGN_LOAN") == 0 && role == MANAGER) {
        int loan_id, employee_id;
        if (count != 5 || !parse_id(fields[3], &loan_id) || !parse_id(fields[4], &employee_id)) {
            reply(out, "ERROR", "Loan or employee ID is invalid."); return;
        }
        Loan *loan = find_loan(state, loan_id);
        User *employee = find_user(state, employee_id);
        if (!loan || strcmp(loan->status, "PENDING") != 0) { reply(out, "ERROR", "Pending loan not found."); return; }
        if (!employee || employee->role != EMPLOYEE) { reply(out, "ERROR", "Employee not found."); return; }
        loan->employee_id = employee_id; snprintf(loan->status, sizeof(loan->status), "ASSIGNED");
        *changed = true; reply(out, "SUCCESS", "Loan assigned to employee."); return;
    }

    if (strcmp(fields[0], "REVIEW_FEEDBACK") == 0 && role == MANAGER) {
        size_t used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        if (state->feedback_count == 0) { append_text(out, RESPONSE_SIZE, &used, "No customer feedback."); return; }
        for (size_t i = 0; i < state->feedback_count; i++) {
            char row[384];
            snprintf(row, sizeof(row), "Feedback %d | Customer %d | %s\n",
                     state->feedback[i].id, state->feedback[i].customer_id, state->feedback[i].text);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
        }
        return;
    }

    if (strcmp(fields[0], "ADD_EMPLOYEE") == 0 && role == ADMIN) {
        if (count != 5 || !safe_field(fields[3]) || !safe_field(fields[4]) || !*fields[3] || !*fields[4] ||
            strlen(fields[3]) >= USERNAME_SIZE || strlen(fields[4]) >= PASSWORD_SIZE) {
            reply(out, "ERROR", "Invalid employee username or password."); return;
        }
        if (find_username(state, fields[3])) { reply(out, "ERROR", "Username already exists."); return; }
        if (state->user_count >= MAX_USERS) { reply(out, "ERROR", "User capacity reached."); return; }
        int id = next_user_id(state); add_user(state, id, EMPLOYEE, fields[3], fields[4]);
        *changed = true; snprintf(out, RESPONSE_SIZE, "SUCCESS|Employee created. User ID: %d", id); return;
    }

    if (strcmp(fields[0], "MOD_USER_DETAILS") == 0 && role == ADMIN) {
        int target_id;
        if (count != 5 || !parse_id(fields[3], &target_id) || !safe_field(fields[4]) || !*fields[4] ||
            strlen(fields[4]) >= PASSWORD_SIZE) { reply(out, "ERROR", "Invalid user ID or password."); return; }
        User *target = find_user(state, target_id);
        if (!target) { reply(out, "ERROR", "User not found."); return; }
        snprintf(target->password, sizeof(target->password), "%s", fields[4]);
        *changed = true; reply(out, "SUCCESS", "User password updated."); return;
    }

    if (strcmp(fields[0], "MANAGE_ROLES") == 0 && role == ADMIN) {
        int target_id, new_role;
        if (count != 5 || !parse_id(fields[3], &target_id) || !parse_id(fields[4], &new_role) ||
            new_role < CUSTOMER || new_role > ADMIN) { reply(out, "ERROR", "User ID or role is invalid."); return; }
        User *target = find_user(state, target_id);
        if (!target) { reply(out, "ERROR", "User not found."); return; }
        if (target->role == CUSTOMER && new_role != CUSTOMER) {
            Account *account = find_account(state, target_id);
            if (account) account->active = false;
        } else if (target->role != CUSTOMER && new_role == CUSTOMER) {
            Account *account = find_account(state, target_id);
            if (account) {
                account->active = true;
            } else {
                if (state->account_count >= MAX_ACCOUNTS) { reply(out, "ERROR", "Account capacity reached."); return; }
                state->accounts[state->account_count++] = (Account){ target_id, 0, true };
            }
        }
        target->role = new_role;
        Session *old_session = find_session(target_id);
        if (old_session) old_session->active = false;
        *changed = true; reply(out, "SUCCESS", "User role updated."); return;
    }

    reply(out, "ERROR", "Unknown command or insufficient role permissions.");
}

static void *client_thread(void *argument) {
    int socket_fd = *(int *)argument;
    char request[REQUEST_SIZE];
    char response[RESPONSE_SIZE];
    ssize_t received;
    free(argument);
    received = recv(socket_fd, request, sizeof(request) - 1, 0);
    if (received <= 0) { close(socket_fd); return NULL; }
    request[received] = '\0';
    request[strcspn(request, "\r\n")] = '\0';

    pthread_mutex_lock(&process_mutex);
    int lock_fd = lock_database();
    if (lock_fd < 0) {
        reply(response, "ERROR", "Could not lock database.");
    } else {
        State *state = calloc(1, sizeof(*state));
        if (!state) {
            reply(response, "ERROR", "Not enough memory to process request.");
        } else {
            int loaded = load_state(state);
            if (loaded < 0) {
                reply(response, "ERROR", "Could not read database.");
            } else {
                bool changed = loaded == 1;
                handle_request(state, request, response, &changed);
                if (changed && save_state(state) != 0)
                    reply(response, "ERROR", "Could not commit database update.");
            }
            free(state);
        }
        unlock_database(lock_fd);
    }
    pthread_mutex_unlock(&process_mutex);

    size_t remaining = strlen(response);
    const char *cursor = response;
    while (remaining > 0) {
        ssize_t sent = send(socket_fd, cursor, remaining, 0);
        if (sent <= 0) break;
        cursor += sent;
        remaining -= (size_t)sent;
    }
    close(socket_fd);
    return NULL;
}

static int initialize_paths(const char *database) {
    if (strlen(database) >= sizeof(database_path)) return -1;
    snprintf(database_path, sizeof(database_path), "%s", database);
    if (snprintf(lock_path, sizeof(lock_path), "%s.lock", database) >= (int)sizeof(lock_path)) return -1;
    if (snprintf(temporary_path, sizeof(temporary_path), "%s.tmp", database) >= (int)sizeof(temporary_path)) return -1;
    pthread_mutex_lock(&process_mutex);
    int lock_fd = lock_database();
    if (lock_fd < 0) { pthread_mutex_unlock(&process_mutex); return -1; }
    State *state = calloc(1, sizeof(*state));
    int result = -1;
    if (state) {
        int loaded = load_state(state);
        result = loaded < 0 ? -1 : (loaded == 1 ? save_state(state) : 0);
        if (result == 0 && chmod(database_path, S_IRUSR | S_IWUSR) != 0) result = -1;
        free(state);
    }
    unlock_database(lock_fd);
    pthread_mutex_unlock(&process_mutex);
    return result;
}

int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    int server_fd;
    struct sockaddr_in address;
    int reuse = 1;
    const char *db = "bank.txt";
    if (argc > 1) {
        char *end = NULL;
        long value = strtol(argv[1], &end, 10);
        if (*end || value < 1 || value > 65535) {
            fprintf(stderr, "Usage: %s [port] [database-path]\n", argv[0]); return 1;
        }
        port = (int)value;
    }
    if (argc > 2) db = argv[2];
    if (argc > 3 || initialize_paths(db) != 0) {
        fprintf(stderr, "Could not initialize database at %s\n", db); return 1;
    }
    signal(SIGPIPE, SIG_IGN);
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); return 1; }
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons((unsigned short)port);
    if (bind(server_fd, (struct sockaddr *)&address, sizeof(address)) < 0 || listen(server_fd, 32) < 0) {
        perror("bind/listen"); close(server_fd); return 1;
    }
    printf("Bank server listening on port %d; database: %s\n", port, database_path);
    printf("Demo logins: admin/admin123, manager/manager123, employee/employee123, customer/customer123\n");
    for (;;) {
        int *client_fd = malloc(sizeof(*client_fd));
        pthread_t thread;
        if (!client_fd) continue;
        *client_fd = accept(server_fd, NULL, NULL);
        if (*client_fd < 0) { free(client_fd); if (errno == EINTR) continue; perror("accept"); continue; }
        if (pthread_create(&thread, NULL, client_thread, client_fd) != 0) {
            close(*client_fd); free(client_fd); continue;
        }
        pthread_detach(thread);
    }
    close(server_fd);
    return 0;
}
