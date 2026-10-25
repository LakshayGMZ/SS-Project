#define _POSIX_C_SOURCE 200809L
#include "request_handler.h"
#include "session_manager.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define MAX_FIELDS 8

typedef struct {
    char *values[MAX_FIELDS];
    size_t count;
} RequestFields;

static void reply(char *out, const char *status, const char *message) {
    snprintf(out, RESPONSE_SIZE, "%s|%s", status, message ? message : "");
}

static bool safe_field(const char *value) {
    return value && !strchr(value, '|') && !strchr(value, '\n') && !strchr(value, '\r');
}

static bool parse_id(const char *value, int *id) {
    char *end = NULL;
    errno = 0;
    long parsed = value && *value ? strtol(value, &end, 10) : 0;
    if (errno || !value || !*value || *end || parsed < 1 || parsed > INT_MAX) return false;
    *id = (int)parsed;
    return true;
}

static bool parse_money(const char *value, long long *amount, bool allow_zero) {
    unsigned long long whole = 0;
    unsigned int fraction = 0;
    int fraction_digits = 0;
    bool has_whole_digit = false;
    const char *cursor = value;
    if (!value || !*value) return false;
    while (*cursor >= '0' && *cursor <= '9') {
        unsigned int digit = (unsigned int)(*cursor - '0');
        unsigned long long limit = (unsigned long long)LLONG_MAX / 100ULL;
        if (whole > (limit - digit) / 10ULL) return false;
        whole = whole * 10ULL + digit;
        has_whole_digit = true;
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
    if (!has_whole_digit || *cursor != '\0') return false;
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

static void split_request(char *request, RequestFields *fields) {
    char *save = NULL;
    fields->count = 0;
    for (char *part = strtok_r(request, "|", &save); part && fields->count < MAX_FIELDS;
         part = strtok_r(NULL, "|", &save))
        fields->values[fields->count++] = part;
}

static bool append_text(char *out, size_t size, size_t *used, const char *text) {
    int written = snprintf(out + *used, size - *used, "%s", text);
    if (written < 0 || (size_t)written >= size - *used) return false;
    *used += (size_t)written;
    return true;
}

static int get_user(Database *database, int id, User *user) {
    DbRecord record;
    int result = db_read_record(database, DB_USERS, id, &record);
    if (result == 1) *user = record.user;
    return result;
}

static int find_user(Database *database, const char *username, User *user) {
    return db_find_user(database, username, user);
}

static int get_account_slot(Database *database, int user_id, int *record_id, Account *account) {
    return db_find_account(database, user_id, record_id, account);
}

static int get_account(Database *database, int user_id, Account *account) {
    int record_id;
    return get_account_slot(database, user_id, &record_id, account);
}

static int get_transaction_count(Database *database, size_t *count) {
    return db_table_count(database, DB_TRANSACTIONS, count);
}

static int append_transaction(DbTransaction *transaction, int customer_id, const char *type,
                              long long amount, const char *details) {
    DbRecord record = {0};
    DbRecord *stored = NULL;
    int id;
    time_t now = time(NULL);
    struct tm parts;
    localtime_r(&now, &parts);
    record.transaction.customer_id = customer_id;
    record.transaction.amount = amount;
    snprintf(record.transaction.type, sizeof(record.transaction.type), "%s", type);
    strftime(record.transaction.timestamp, sizeof(record.transaction.timestamp),
             "%Y-%m-%d %H:%M:%S", &parts);
    snprintf(record.transaction.details, sizeof(record.transaction.details), "%s", details);
    if (db_tx_append(transaction, DB_TRANSACTIONS, &record, &id, &stored) != 0) return -1;
    return id;
}

static int ensure_authorized(Database *database, int role, int user_id, char *out) {
    User user;
    if (!sessions_authorize(user_id, role)) {
        reply(out, "ERROR", "Please log in again.");
        return 0;
    }
    if (get_user(database, user_id, &user) != 1 || user.role != role) {
        sessions_invalidate(user_id);
        reply(out, "ERROR", "Please log in again.");
        return 0;
    }
    return 1;
}

static int add_new_customer(Database *database, const char *username, const char *password,
                            long long initial_balance, bool register_and_login,
                            char *out) {
    DbTransaction *transaction = db_tx_begin(database);
    DbRecord *stored_user = NULL;
    DbRecord user_record = {0};
    DbRecord account_record = {0};
    User existing;
    int user_id;
    bool session_started = false;

    if (!transaction) { reply(out, "ERROR", "Database is unavailable."); return 0; }
    if (db_tx_reserve_append(transaction, DB_USERS) != 0) {
        db_tx_abort(transaction); reply(out, "ERROR", "User capacity reached."); return 0;
    }
    int duplicate = find_user(database, username, &existing);
    if (duplicate < 0) { db_tx_abort(transaction); reply(out, "ERROR", "Could not read users."); return 0; }
    if (duplicate) { db_tx_abort(transaction); reply(out, "ERROR", "Username already exists."); return 0; }

    user_record.user.role = CUSTOMER;
    snprintf(user_record.user.username, sizeof(user_record.user.username), "%s", username);
    snprintf(user_record.user.password, sizeof(user_record.user.password), "%s", password);
    if (db_tx_append(transaction, DB_USERS, &user_record, &user_id, &stored_user) != 0) {
        db_tx_abort(transaction); reply(out, "ERROR", "Could not create user record."); return 0;
    }
    account_record.account.user_id = user_id;
    account_record.account.balance = initial_balance;
    account_record.account.active = true;
    if (db_tx_create_account(transaction, user_id, &account_record.account) != 0) {
        db_tx_abort(transaction); reply(out, "ERROR", "Could not create account record."); return 0;
    }
    if (initial_balance > 0) {
        if (db_tx_reserve_append(transaction, DB_TRANSACTIONS) != 0 ||
            append_transaction(transaction, user_id, "OPENING BALANCE", initial_balance,
                               "Initial account balance") < 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Could not record opening balance."); return 0;
        }
    }
    if (register_and_login) {
        session_started = sessions_start(user_id, CUSTOMER);
        if (!session_started) {
            db_tx_abort(transaction); reply(out, "ERROR", "Could not create session."); return 0;
        }
    }
    if (db_tx_commit(transaction) != 0) {
        if (session_started) sessions_logout(user_id);
        reply(out, "ERROR", "Could not commit account creation."); return 0;
    }
    if (register_and_login)
        snprintf(out, RESPONSE_SIZE, "SUCCESS|%d|Account created.", user_id);
    else
        snprintf(out, RESPONSE_SIZE, "SUCCESS|Customer created. User ID: %d", user_id);
    return 1;
}

static int process_customer(Database *database, const RequestFields *fields,
                            int role, int user_id, char *out) {
    const char *command = fields->values[0];
    if (strcmp(command, "VIEW_BALANCE") == 0 && role == CUSTOMER) {
        Account account;
        char amount[64];
        if (get_account(database, user_id, &account) != 1 || !account.active) {
            reply(out, "ERROR", "Account is inactive."); return 1;
        }
        format_amount(account.balance, amount, sizeof(amount));
        snprintf(out, RESPONSE_SIZE, "SUCCESS|Available balance: %s", amount);
        return 1;
    }

    if ((strcmp(command, "DEPOSIT") == 0 || strcmp(command, "WITHDRAW") == 0) && role == CUSTOMER) {
        long long amount;
        DbTransaction *transaction;
        DbRecord *account_record;
        Account account_snapshot;
        int account_record_id;
        bool deposit = strcmp(command, "DEPOSIT") == 0;
        if (fields->count != 4 || !parse_amount(fields->values[3], &amount)) {
            reply(out, "ERROR", "Enter a valid positive amount."); return 1;
        }
        if (get_account_slot(database, user_id, &account_record_id, &account_snapshot) != 1 ||
            !account_snapshot.active) {
            reply(out, "ERROR", "Account is inactive."); return 1;
        }
        transaction = db_tx_begin(database);
        if (!transaction || db_tx_get(transaction, DB_ACCOUNTS, account_record_id, true, &account_record) != 1 ||
            account_record->account.user_id != user_id || !account_record->account.active) {
            db_tx_abort(transaction); reply(out, "ERROR", "Account is inactive."); return 1;
        }
        size_t tx_count;
        if (get_transaction_count(database, &tx_count) != 0 ||
            tx_count >= db_table_limit(DB_TRANSACTIONS) ||
            db_tx_reserve_append(transaction, DB_TRANSACTIONS) != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Transaction capacity reached."); return 1;
        }
        if (!deposit && amount > account_record->account.balance) {
            db_tx_abort(transaction); reply(out, "ERROR", "Insufficient funds."); return 1;
        }
        if (deposit && account_record->account.balance > LLONG_MAX - amount) {
            db_tx_abort(transaction); reply(out, "ERROR", "Balance limit exceeded."); return 1;
        }
        account_record->account.balance += deposit ? amount : -amount;
        if (append_transaction(transaction, user_id, deposit ? "DEPOSIT" : "WITHDRAWAL", amount,
                               deposit ? "Cash deposit" : "Cash withdrawal") < 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Could not record transaction."); return 1;
        }
        if (db_tx_commit(transaction) != 0) {
            reply(out, "ERROR", "Could not commit transaction."); return 1;
        }
        char amount_text[64];
        format_amount(amount, amount_text, sizeof(amount_text));
        snprintf(out, RESPONSE_SIZE, "SUCCESS|%s of %s completed.",
                 deposit ? "Deposit" : "Withdrawal", amount_text);
        return 1;
    }

    if (strcmp(command, "TRANSFER") == 0 && role == CUSTOMER) {
        int target_id;
        long long amount;
        DbTransaction *transaction;
        DbRecord *source_record = NULL, *target_record = NULL;
        if (fields->count != 5 || !parse_id(fields->values[3], &target_id) ||
            !parse_amount(fields->values[4], &amount)) {
            reply(out, "ERROR", "Enter a valid target account and positive amount."); return 1;
        }
        if (target_id == user_id) { reply(out, "ERROR", "Cannot transfer to the same account."); return 1; }
        int source_account_id, target_account_id;
        Account source_snapshot, target_snapshot;
        if (get_account_slot(database, user_id, &source_account_id, &source_snapshot) != 1 ||
            get_account_slot(database, target_id, &target_account_id, &target_snapshot) != 1) {
            reply(out, "ERROR", "Target customer account is unavailable."); return 1;
        }
        transaction = db_tx_begin(database);
        if (!transaction) { reply(out, "ERROR", "Database is unavailable."); return 1; }
        int first_id = source_account_id < target_account_id ? source_account_id : target_account_id;
        int second_id = source_account_id < target_account_id ? target_account_id : source_account_id;
        DbRecord *first_record = NULL, *second_record = NULL;
        if (db_tx_get(transaction, DB_ACCOUNTS, first_id, true, &first_record) != 1 ||
            db_tx_get(transaction, DB_ACCOUNTS, second_id, true, &second_record) != 1) {
            db_tx_abort(transaction); reply(out, "ERROR", "Target customer account is unavailable."); return 1;
        }
        source_record = source_account_id == first_id ? first_record : second_record;
        target_record = target_account_id == first_id ? first_record : second_record;
        if (source_record->account.user_id != user_id || target_record->account.user_id != target_id ||
            !source_record->account.active || !target_record->account.active) {
            db_tx_abort(transaction); reply(out, "ERROR", "Target customer account is unavailable."); return 1;
        }
        if (source_record->account.balance < amount) {
            db_tx_abort(transaction); reply(out, "ERROR", "Insufficient funds."); return 1;
        }
        if (target_record->account.balance > LLONG_MAX - amount) {
            db_tx_abort(transaction); reply(out, "ERROR", "Target balance limit exceeded."); return 1;
        }
        if (db_tx_reserve_append(transaction, DB_TRANSACTIONS) != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Transaction capacity reached."); return 1;
        }
        source_record->account.balance -= amount;
        target_record->account.balance += amount;
        char details[TEXT_SIZE];
        snprintf(details, sizeof(details), "Transfer to customer %d", target_id);
        if (append_transaction(transaction, user_id, "OUTGOING TRANSFER", amount, details) < 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Could not record transfer."); return 1;
        }
        snprintf(details, sizeof(details), "Transfer from customer %d", user_id);
        if (append_transaction(transaction, target_id, "INCOMING TRANSFER", amount, details) < 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Could not record transfer."); return 1;
        }
        if (db_tx_commit(transaction) != 0) {
            reply(out, "ERROR", "Could not commit transfer."); return 1;
        }
        char amount_text[64];
        format_amount(amount, amount_text, sizeof(amount_text));
        snprintf(out, RESPONSE_SIZE, "SUCCESS|Transfer of %s to account %d completed.", amount_text, target_id);
        return 1;
    }

    if (strcmp(command, "APPLY_LOAN") == 0 && role == CUSTOMER) {
        long long amount;
        DbTransaction *transaction;
        DbRecord *account;
        Account account_snapshot;
        int account_record_id;
        DbRecord loan_record = {0};
        DbRecord *stored_loan;
        int loan_id;
        if (fields->count != 4 || !parse_amount(fields->values[3], &amount)) {
            reply(out, "ERROR", "Enter a valid positive loan amount."); return 1;
        }
        if (get_account_slot(database, user_id, &account_record_id, &account_snapshot) != 1 ||
            !account_snapshot.active) {
            reply(out, "ERROR", "Account is inactive."); return 1;
        }
        transaction = db_tx_begin(database);
        if (!transaction || db_tx_get(transaction, DB_ACCOUNTS, account_record_id, false, &account) != 1 ||
            account->account.user_id != user_id || !account->account.active) {
            db_tx_abort(transaction); reply(out, "ERROR", "Account is inactive."); return 1;
        }
        loan_record.loan.customer_id = user_id;
        loan_record.loan.amount = amount;
        loan_record.loan.employee_id = 0;
        snprintf(loan_record.loan.status, sizeof(loan_record.loan.status), "PENDING");
        if (db_tx_append(transaction, DB_LOANS, &loan_record, &loan_id, &stored_loan) != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Loan capacity reached."); return 1;
        }
        if (db_tx_commit(transaction) != 0) {
            reply(out, "ERROR", "Could not submit loan application."); return 1;
        }
        snprintf(out, RESPONSE_SIZE, "SUCCESS|Loan application submitted. Loan ID: %d", loan_id);
        return 1;
    }

    if (strcmp(command, "VIEW_HISTORY") == 0 && role == CUSTOMER) {
        size_t count, used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        if (db_table_count(database, DB_TRANSACTIONS, &count) != 0) {
            reply(out, "ERROR", "Could not read transaction history."); return 1;
        }
        for (size_t id = 1; id <= count; id++) {
            DbRecord record;
            if (db_read_record(database, DB_TRANSACTIONS, (int)id, &record) != 1) continue;
            Transaction *entry = &record.transaction;
            if (entry->customer_id != user_id) continue;
            char amount[64], row[512];
            format_amount(entry->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "%s | %s | %s | %s\n", entry->timestamp,
                     entry->type, amount, entry->details);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No transactions yet.");
        return 1;
    }

    if (strcmp(command, "ADD_FEEDBACK") == 0 && role == CUSTOMER) {
        DbTransaction *transaction;
        DbRecord record = {0};
        DbRecord *stored;
        int id;
        if (fields->count != 4 || !safe_field(fields->values[3]) || !*fields->values[3] ||
            strlen(fields->values[3]) >= TEXT_SIZE) {
            reply(out, "ERROR", "Feedback must be 1 to 255 characters and cannot contain '|'."); return 1;
        }
        transaction = db_tx_begin(database);
        if (!transaction) { reply(out, "ERROR", "Database is unavailable."); return 1; }
        record.feedback.customer_id = user_id;
        snprintf(record.feedback.text, sizeof(record.feedback.text), "%s", fields->values[3]);
        if (db_tx_append(transaction, DB_FEEDBACK, &record, &id, &stored) != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Feedback capacity reached."); return 1;
        }
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not save feedback.");
        else reply(out, "SUCCESS", "Feedback submitted.");
        return 1;
    }
    return 0;
}

static int process_employee(Database *database, const RequestFields *fields,
                            int role, int user_id, char *out) {
    const char *command = fields->values[0];
    if (strcmp(command, "ADD_CUSTOMER") == 0 && role == EMPLOYEE) {
        long long balance;
        if (fields->count != 6 || !safe_field(fields->values[3]) || !safe_field(fields->values[4]) ||
            !*fields->values[3] || !*fields->values[4] ||
            strlen(fields->values[3]) >= USERNAME_SIZE || strlen(fields->values[4]) >= PASSWORD_SIZE ||
            !parse_balance(fields->values[5], &balance)) {
            reply(out, "ERROR", "Invalid customer details or opening balance."); return 1;
        }
        return add_new_customer(database, fields->values[3], fields->values[4], balance, false, out);
    }

    if (strcmp(command, "MOD_CUSTOMER") == 0 && role == EMPLOYEE) {
        int target_id;
        DbTransaction *transaction;
        DbRecord *target;
        if (fields->count != 5 || !parse_id(fields->values[3], &target_id) ||
            !safe_field(fields->values[4]) || !*fields->values[4] ||
            strlen(fields->values[4]) >= PASSWORD_SIZE) {
            reply(out, "ERROR", "Invalid customer ID or password."); return 1;
        }
        transaction = db_tx_begin(database);
        if (!transaction || db_tx_get(transaction, DB_USERS, target_id, true, &target) != 1 ||
            target->user.role != CUSTOMER) {
            db_tx_abort(transaction); reply(out, "ERROR", "Customer not found."); return 1;
        }
        snprintf(target->user.password, sizeof(target->user.password), "%s", fields->values[4]);
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not update customer.");
        else reply(out, "SUCCESS", "Customer password updated.");
        return 1;
    }

    if (strcmp(command, "VIEW_ASSIGNED_LOANS") == 0 && role == EMPLOYEE) {
        size_t count, used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        if (db_table_count(database, DB_LOANS, &count) != 0) {
            reply(out, "ERROR", "Could not read loan applications."); return 1;
        }
        for (size_t id = 1; id <= count; id++) {
            DbRecord record;
            if (db_read_record(database, DB_LOANS, (int)id, &record) != 1) continue;
            Loan *loan = &record.loan;
            if (loan->employee_id != user_id || strcmp(loan->status, "ASSIGNED") != 0) continue;
            char amount[64], row[256];
            format_amount(loan->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "Loan %d | Customer %d | Amount %s | %s\n",
                     loan->id, loan->customer_id, amount, loan->status);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No assigned loan applications.");
        return 1;
    }

    if (strcmp(command, "PROCESS_LOAN") == 0 && role == EMPLOYEE) {
        int loan_id;
        if (fields->count != 5 || !parse_id(fields->values[3], &loan_id) ||
            (strcmp(fields->values[4], "APPROVED") != 0 && strcmp(fields->values[4], "REJECTED") != 0)) {
            reply(out, "ERROR", "Loan ID or decision is invalid."); return 1;
        }
        DbRecord initial_loan;
        if (db_read_record(database, DB_LOANS, loan_id, &initial_loan) != 1) {
            reply(out, "ERROR", "Loan is not assigned to this employee."); return 1;
        }
        bool approve = strcmp(fields->values[4], "APPROVED") == 0;
        int account_record_id = 0;
        Account account_snapshot;
        if (approve && get_account_slot(database, initial_loan.loan.customer_id,
                                        &account_record_id, &account_snapshot) != 1) {
            reply(out, "ERROR", "Customer account is unavailable."); return 1;
        }
        DbTransaction *transaction = db_tx_begin(database);
        DbRecord *account = NULL, *loan = NULL;
        if (!transaction) { reply(out, "ERROR", "Database is unavailable."); return 1; }
        if (approve && db_tx_get(transaction, DB_ACCOUNTS, account_record_id, true, &account) != 1) {
            db_tx_abort(transaction); reply(out, "ERROR", "Customer account is unavailable."); return 1;
        }
        if (db_tx_get(transaction, DB_LOANS, loan_id, true, &loan) != 1 ||
            loan->loan.employee_id != user_id || strcmp(loan->loan.status, "ASSIGNED") != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Loan is not assigned to this employee."); return 1;
        }
        if (approve) {
            if (account->account.user_id != loan->loan.customer_id || !account->account.active) {
                db_tx_abort(transaction); reply(out, "ERROR", "Customer account is unavailable."); return 1;
            }
            if (account->account.balance > LLONG_MAX - loan->loan.amount) {
                db_tx_abort(transaction); reply(out, "ERROR", "Customer balance limit exceeded."); return 1;
            }
            if (db_tx_reserve_append(transaction, DB_TRANSACTIONS) != 0 ||
                account->account.balance > LLONG_MAX - loan->loan.amount) {
                db_tx_abort(transaction); reply(out, "ERROR", "Could not apply loan."); return 1;
            }
            account->account.balance += loan->loan.amount;
            char details[TEXT_SIZE];
            snprintf(details, sizeof(details), "Approved loan %d", loan_id);
            if (append_transaction(transaction, loan->loan.customer_id, "LOAN CREDIT",
                                   loan->loan.amount, details) < 0) {
                db_tx_abort(transaction); reply(out, "ERROR", "Could not record loan credit."); return 1;
            }
        }
        snprintf(loan->loan.status, sizeof(loan->loan.status), "%s", fields->values[4]);
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not process loan.");
        else snprintf(out, RESPONSE_SIZE, "SUCCESS|Loan %d %s.", loan_id,
                      approve ? "approved" : "rejected");
        return 1;
    }

    if (strcmp(command, "VIEW_PASSBOOK") == 0 && role == EMPLOYEE) {
        int customer_id;
        Account account;
        if (fields->count != 4 || !parse_id(fields->values[3], &customer_id) ||
            get_account(database, customer_id, &account) != 1) {
            reply(out, "ERROR", "Customer account not found."); return 1;
        }
        size_t count, used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        if (db_table_count(database, DB_TRANSACTIONS, &count) != 0) {
            reply(out, "ERROR", "Could not read transactions."); return 1;
        }
        for (size_t id = 1; id <= count; id++) {
            DbRecord record;
            if (db_read_record(database, DB_TRANSACTIONS, (int)id, &record) != 1) continue;
            Transaction *entry = &record.transaction;
            if (entry->customer_id != customer_id) continue;
            char amount[64], row[512];
            format_amount(entry->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "%s | %s | %s | %s\n", entry->timestamp,
                     entry->type, amount, entry->details);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No transactions yet.");
        return 1;
    }
    return 0;
}

static int process_manager(Database *database, const RequestFields *fields,
                           int role, char *out) {
    const char *command = fields->values[0];
    if (strcmp(command, "ACT_DEACT_ACC") == 0 && role == MANAGER) {
        int target_id;
        if (fields->count != 5 || !parse_id(fields->values[3], &target_id) ||
            (strcmp(fields->values[4], "ACTIVATE") != 0 && strcmp(fields->values[4], "DEACTIVATE") != 0)) {
            reply(out, "ERROR", "Customer ID or account status is invalid."); return 1;
        }
        int account_record_id;
        Account account_snapshot;
        if (get_account_slot(database, target_id, &account_record_id, &account_snapshot) != 1) {
            reply(out, "ERROR", "Customer account not found."); return 1;
        }
        DbTransaction *transaction = db_tx_begin(database);
        DbRecord *user = NULL, *account = NULL;
        if (!transaction || db_tx_get(transaction, DB_USERS, target_id, false, &user) != 1 ||
            user->user.role != CUSTOMER ||
            db_tx_get(transaction, DB_ACCOUNTS, account_record_id, true, &account) != 1 ||
            account->account.user_id != target_id) {
            db_tx_abort(transaction); reply(out, "ERROR", "Customer account not found."); return 1;
        }
        account->account.active = strcmp(fields->values[4], "ACTIVATE") == 0;
        bool active = account->account.active;
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not update account status.");
        else reply(out, "SUCCESS", active ? "Account activated." : "Account deactivated.");
        return 1;
    }

    if (strcmp(command, "VIEW_LOANS") == 0 && role == MANAGER) {
        size_t count, used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        bool found = false;
        if (db_table_count(database, DB_LOANS, &count) != 0) {
            reply(out, "ERROR", "Could not read loan applications."); return 1;
        }
        for (size_t id = 1; id <= count; id++) {
            DbRecord record;
            if (db_read_record(database, DB_LOANS, (int)id, &record) != 1) continue;
            Loan *loan = &record.loan;
            if (strcmp(loan->status, "PENDING") != 0) continue;
            char amount[64], row[256];
            format_amount(loan->amount, amount, sizeof(amount));
            snprintf(row, sizeof(row), "Loan %d | Customer %d | Amount %s | %s\n",
                     loan->id, loan->customer_id, amount, loan->status);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
            found = true;
        }
        if (!found) append_text(out, RESPONSE_SIZE, &used, "No pending loan applications.");
        return 1;
    }

    if (strcmp(command, "ASSIGN_LOAN") == 0 && role == MANAGER) {
        int loan_id, employee_id;
        if (fields->count != 5 || !parse_id(fields->values[3], &loan_id) ||
            !parse_id(fields->values[4], &employee_id)) {
            reply(out, "ERROR", "Loan or employee ID is invalid."); return 1;
        }
        DbTransaction *transaction = db_tx_begin(database);
        DbRecord *employee = NULL, *loan = NULL;
        if (!transaction || db_tx_get(transaction, DB_USERS, employee_id, false, &employee) != 1 ||
            employee->user.role != EMPLOYEE ||
            db_tx_get(transaction, DB_LOANS, loan_id, true, &loan) != 1 ||
            strcmp(loan->loan.status, "PENDING") != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Pending loan or employee not found."); return 1;
        }
        loan->loan.employee_id = employee_id;
        snprintf(loan->loan.status, sizeof(loan->loan.status), "ASSIGNED");
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not assign loan.");
        else reply(out, "SUCCESS", "Loan assigned to employee.");
        return 1;
    }

    if (strcmp(command, "REVIEW_FEEDBACK") == 0 && role == MANAGER) {
        size_t count, used = (size_t)snprintf(out, RESPONSE_SIZE, "SUCCESS|");
        if (db_table_count(database, DB_FEEDBACK, &count) != 0) {
            reply(out, "ERROR", "Could not read feedback."); return 1;
        }
        if (count == 0) { append_text(out, RESPONSE_SIZE, &used, "No customer feedback."); return 1; }
        for (size_t id = 1; id <= count; id++) {
            DbRecord record;
            if (db_read_record(database, DB_FEEDBACK, (int)id, &record) != 1) continue;
            char row[384];
            snprintf(row, sizeof(row), "Feedback %d | Customer %d | %s\n",
                     record.feedback.id, record.feedback.customer_id, record.feedback.text);
            if (!append_text(out, RESPONSE_SIZE, &used, row)) break;
        }
        return 1;
    }
    return 0;
}

static int process_admin(Database *database, const RequestFields *fields,
                         int role, char *out) {
    const char *command = fields->values[0];
    if (strcmp(command, "ADD_EMPLOYEE") == 0 && role == ADMIN) {
        DbTransaction *transaction;
        DbRecord user = {0};
        DbRecord *stored_user;
        User existing;
        int user_id_new;
        if (fields->count != 5 || !safe_field(fields->values[3]) || !safe_field(fields->values[4]) ||
            !*fields->values[3] || !*fields->values[4] ||
            strlen(fields->values[3]) >= USERNAME_SIZE || strlen(fields->values[4]) >= PASSWORD_SIZE) {
            reply(out, "ERROR", "Invalid employee username or password."); return 1;
        }
        transaction = db_tx_begin(database);
        if (!transaction || db_tx_reserve_append(transaction, DB_USERS) != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "User capacity reached."); return 1;
        }
        int duplicate = find_user(database, fields->values[3], &existing);
        if (duplicate != 0) {
            db_tx_abort(transaction);
            reply(out, "ERROR", duplicate > 0 ? "Username already exists." : "Could not read users.");
            return 1;
        }
        user.user.role = EMPLOYEE;
        snprintf(user.user.username, sizeof(user.user.username), "%s", fields->values[3]);
        snprintf(user.user.password, sizeof(user.user.password), "%s", fields->values[4]);
        if (db_tx_append(transaction, DB_USERS, &user, &user_id_new, &stored_user) != 0) {
            db_tx_abort(transaction); reply(out, "ERROR", "Could not create employee."); return 1;
        }
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not save employee.");
        else snprintf(out, RESPONSE_SIZE, "SUCCESS|Employee created. User ID: %d", user_id_new);
        return 1;
    }

    if (strcmp(command, "MOD_USER_DETAILS") == 0 && role == ADMIN) {
        int target_id;
        if (fields->count != 5 || !parse_id(fields->values[3], &target_id) ||
            !safe_field(fields->values[4]) || !*fields->values[4] ||
            strlen(fields->values[4]) >= PASSWORD_SIZE) {
            reply(out, "ERROR", "Invalid user ID or password."); return 1;
        }
        DbTransaction *transaction = db_tx_begin(database);
        DbRecord *target;
        if (!transaction || db_tx_get(transaction, DB_USERS, target_id, true, &target) != 1) {
            db_tx_abort(transaction); reply(out, "ERROR", "User not found."); return 1;
        }
        snprintf(target->user.password, sizeof(target->user.password), "%s", fields->values[4]);
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not update user.");
        else reply(out, "SUCCESS", "User password updated.");
        return 1;
    }

    if (strcmp(command, "MANAGE_ROLES") == 0 && role == ADMIN) {
        int target_id, new_role;
        if (fields->count != 5 || !parse_id(fields->values[3], &target_id) ||
            !parse_id(fields->values[4], &new_role) || new_role < CUSTOMER || new_role > ADMIN) {
            reply(out, "ERROR", "User ID or role is invalid."); return 1;
        }
        DbTransaction *transaction = db_tx_begin(database);
        DbRecord *target = NULL, *account = NULL;
        if (!transaction || db_tx_get(transaction, DB_USERS, target_id, true, &target) != 1) {
            db_tx_abort(transaction); reply(out, "ERROR", "User not found."); return 1;
        }
        int previous_role = target->user.role;
        if (previous_role == CUSTOMER && new_role != CUSTOMER) {
            int account_record_id;
            Account account_snapshot;
            if (get_account_slot(database, target_id, &account_record_id, &account_snapshot) != 1 ||
                db_tx_get(transaction, DB_ACCOUNTS, account_record_id, true, &account) != 1 ||
                account->account.user_id != target_id) {
                db_tx_abort(transaction); reply(out, "ERROR", "Customer account not found."); return 1;
            }
            account->account.active = false;
        } else if (previous_role != CUSTOMER && new_role == CUSTOMER) {
            int account_record_id;
            Account account_snapshot;
            int found = get_account_slot(database, target_id, &account_record_id, &account_snapshot);
            if (found < 0) {
                db_tx_abort(transaction); reply(out, "ERROR", "Could not verify customer account."); return 1;
            }
            if (found == 1) {
                if (db_tx_get(transaction, DB_ACCOUNTS, account_record_id, true, &account) != 1 ||
                    account->account.user_id != target_id) {
                    db_tx_abort(transaction); reply(out, "ERROR", "Customer account not found."); return 1;
                }
                account->account.active = true;
            } else {
                Account new_account = { .user_id = target_id, .balance = 0, .active = true };
                if (db_tx_create_account(transaction, target_id, &new_account) != 0) {
                    db_tx_abort(transaction); reply(out, "ERROR", "Account capacity reached."); return 1;
                }
            }
        }
        target->user.role = new_role;
        if (db_tx_commit(transaction) != 0) reply(out, "ERROR", "Could not update role.");
        else {
            sessions_invalidate(target_id);
            reply(out, "SUCCESS", "User role updated.");
        }
        return 1;
    }
    return 0;
}

void handle_request(Database *database, char *request, char *response) {
    RequestFields fields = {0};
    int role, user_id;
    split_request(request, &fields);
    if (fields.count == 0) { reply(response, "ERROR", "Empty request."); return; }

    const char *command = fields.values[0];
    if (strcmp(command, "CREATE_CUSTOMER") == 0) {
        long long zero_balance = 0;
        if (fields.count != 4 || !parse_id(fields.values[1], &role) || role != CUSTOMER ||
            !safe_field(fields.values[2]) || !safe_field(fields.values[3]) ||
            !*fields.values[2] || !*fields.values[3] ||
            strlen(fields.values[2]) >= USERNAME_SIZE || strlen(fields.values[3]) >= PASSWORD_SIZE) {
            reply(response, "ERROR", "Invalid registration details."); return;
        }
        add_new_customer(database, fields.values[2], fields.values[3], zero_balance, true, response);
        return;
    }

    if (strcmp(command, "LOGIN") == 0) {
        User user;
        if (fields.count != 4 || !parse_id(fields.values[1], &role) ||
            role < CUSTOMER || role > ADMIN) {
            reply(response, "ERROR", "Invalid login request."); return;
        }
        int found = find_user(database, fields.values[2], &user);
        if (found != 1 || user.role != role || strcmp(user.password, fields.values[3]) != 0) {
            reply(response, "ERROR", "Invalid username, password, or role."); return;
        }
        if (role == CUSTOMER) {
            Account account;
            if (get_account(database, user.id, &account) != 1 || !account.active) {
                reply(response, "ERROR", "Customer account is inactive."); return;
            }
        }
        if (!sessions_start(user.id, role)) {
            reply(response, "ERROR", "This user already has an active session."); return;
        }
        snprintf(response, RESPONSE_SIZE, "SUCCESS|%d|Login successful.", user.id);
        return;
    }

    if (fields.count < 3 || !parse_id(fields.values[1], &role) ||
        !parse_id(fields.values[2], &user_id) || role < CUSTOMER || role > ADMIN) {
        reply(response, "ERROR", "Invalid request fields."); return;
    }
    if (!ensure_authorized(database, role, user_id, response)) return;

    if (strcmp(command, "LOGOUT") == 0) {
        sessions_logout(user_id);
        reply(response, "SUCCESS", "Logged out.");
        return;
    }
    if (strcmp(command, "CHANGE_PASS") == 0) {
        if (fields.count != 4 || !safe_field(fields.values[3]) || !*fields.values[3] ||
            strlen(fields.values[3]) >= PASSWORD_SIZE) {
            reply(response, "ERROR", "Password must be 1 to 49 characters and cannot contain '|'."); return;
        }
        DbTransaction *transaction = db_tx_begin(database);
        DbRecord *user;
        if (!transaction || db_tx_get(transaction, DB_USERS, user_id, true, &user) != 1) {
            db_tx_abort(transaction); reply(response, "ERROR", "User record not found."); return;
        }
        snprintf(user->user.password, sizeof(user->user.password), "%s", fields.values[3]);
        if (db_tx_commit(transaction) != 0) reply(response, "ERROR", "Could not change password.");
        else reply(response, "SUCCESS", "Password changed.");
        return;
    }

    if (process_customer(database, &fields, role, user_id, response)) return;
    if (process_employee(database, &fields, role, user_id, response)) return;
    if (process_manager(database, &fields, role, response)) return;
    if (process_admin(database, &fields, role, response)) return;
    reply(response, "ERROR", "Unknown command or insufficient role permissions.");
}
