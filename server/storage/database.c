#define _POSIX_C_SOURCE 200809L
#include "database.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define DB_MAX_TX_RECORDS 12
#define DB_MAX_ROW_SIZE 400
#define DB_ROW_SHARDS 257
#define WAL_MAGIC 0x42414e4bU
#define WAL_VERSION 2U

#define USER_ROW_SIZE 130
#define ACCOUNT_ROW_SIZE 49
#define LOAN_ROW_SIZE 68
#define TRANSACTION_ROW_SIZE 356
#define FEEDBACK_ROW_SIZE 287

typedef struct {
    uint32_t magic;
    uint32_t version;
    uint32_t count;
    uint32_t checksum;
} WalHeader;

typedef struct {
    uint32_t table;
    int32_t id;
    uint32_t size;
    char row[DB_MAX_ROW_SIZE];
} WalEntry;

typedef struct {
    DbTable table;
    int id;
    int shard;
    bool write_lock;
    bool dirty;
    DbRecord record;
} DbEntry;

typedef struct {
    bool held;
    size_t next_id;
} AppendReservation;

struct Database {
    char directory[PATH_MAX];
    int table_fd[DB_TABLE_COUNT];
    int append_fd[DB_TABLE_COUNT];
    int wal_fd;
    int commit_fd;
    int startup_fd;
    bool healthy;
};

struct DbTransaction {
    Database *database;
    DbEntry entries[DB_MAX_TX_RECORDS];
    size_t entry_count;
    AppendReservation append[DB_TABLE_COUNT];
    bool has_last_key;
    DbTable last_table;
    int last_id;
    bool has_last_append;
    DbTable last_append_table;
};

static const char *table_names[DB_TABLE_COUNT] = {
    "users", "accounts", "loans", "transactions", "feedback"
};

static const size_t table_row_sizes[DB_TABLE_COUNT] = {
    USER_ROW_SIZE, ACCOUNT_ROW_SIZE, LOAN_ROW_SIZE,
    TRANSACTION_ROW_SIZE, FEEDBACK_ROW_SIZE
};

static const size_t table_limits[DB_TABLE_COUNT] = {
    2000, 2000, 4000, 20000, 4000
};

static pthread_once_t lock_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t row_mutexes[DB_ROW_SHARDS];
static pthread_mutex_t append_mutexes[DB_TABLE_COUNT];
static pthread_mutex_t commit_mutex = PTHREAD_MUTEX_INITIALIZER;

static int validate_database_relations(Database *database);

static void initialize_mutexes(void) {
    for (int i = 0; i < DB_ROW_SHARDS; i++)
        pthread_mutex_init(&row_mutexes[i], NULL);
    for (int i = 0; i < DB_TABLE_COUNT; i++)
        pthread_mutex_init(&append_mutexes[i], NULL);
}

static void set_error(char *error, size_t size, const char *message) {
    if (error && size > 0) snprintf(error, size, "%s", message);
}

static int lock_region(int fd, short type, off_t start, off_t length) {
    struct flock lock = {
        .l_type = type,
        .l_whence = SEEK_SET,
        .l_start = start,
        .l_len = length
    };
    int command = type == F_UNLCK ? F_SETLK : F_SETLKW;
    while (fcntl(fd, command, &lock) < 0) {
        if (errno == EINTR && type != F_UNLCK) continue;
        return -1;
    }
    return 0;
}

static int write_all_at(int fd, const void *buffer, size_t size, off_t offset) {
    const char *cursor = buffer;
    size_t written = 0;
    while (written < size) {
        ssize_t result = pwrite(fd, cursor + written, size - written, offset + (off_t)written);
        if (result < 0 && errno == EINTR) continue;
        if (result <= 0) return -1;
        written += (size_t)result;
    }
    return 0;
}

static int read_all_at(int fd, void *buffer, size_t size, off_t offset) {
    char *cursor = buffer;
    size_t received = 0;
    while (received < size) {
        ssize_t result = pread(fd, cursor + received, size - received, offset + (off_t)received);
        if (result < 0 && errno == EINTR) continue;
        if (result <= 0) return -1;
        received += (size_t)result;
    }
    return 0;
}

static size_t record_size(DbTable table) {
    if (table < 0 || table >= DB_TABLE_COUNT) return 0;
    return table_row_sizes[table];
}

static bool blank_account_row(const char *row) {
    for (size_t i = 0; i < ACCOUNT_ROW_SIZE - 1; i++)
        if (row[i] != ' ') return false;
    return row[ACCOUNT_ROW_SIZE - 1] == '\n';
}

size_t db_table_limit(DbTable table) {
    if (table < 0 || table >= DB_TABLE_COUNT) return 0;
    return table_limits[table];
}

static int row_shard(DbTable table, int id) {
    unsigned int hash = (unsigned int)table * 2654435761U + (unsigned int)id;
    return (int)(hash % DB_ROW_SHARDS);
}

static int table_record_count(Database *database, DbTable table, size_t *count) {
    struct stat status;
    size_t size = record_size(table);
    if (!database || !count || size == 0 || fstat(database->table_fd[table], &status) != 0)
        return -1;
    if (status.st_size < 0 || (uint64_t)status.st_size % size != 0) return -1;
    *count = (size_t)status.st_size / size;
    return 0;
}

int db_table_count(Database *database, DbTable table, size_t *count) {
    if (!database || !database->healthy) return -1;
    return table_record_count(database, table, count);
}

static char *trim_right(char *value) {
    size_t length = strlen(value);
    while (length > 0 && value[length - 1] == ' ') value[--length] = '\0';
    return value;
}

static int split_row(char *row, char **fields, size_t expected) {
    char *save = NULL;
    size_t count = 0;
    for (char *field = strtok_r(row, "|", &save); field;
         field = strtok_r(NULL, "|", &save)) {
        if (count >= expected) return -1;
        fields[count++] = field;
    }
    return count == expected ? 0 : -1;
}

static const char *role_name(int role) {
    switch (role) {
        case CUSTOMER: return "CUSTOMER";
        case EMPLOYEE: return "EMPLOYEE";
        case MANAGER: return "MANAGER";
        case ADMIN: return "ADMINISTRATOR";
        default: return NULL;
    }
}

static int role_from_name(const char *name) {
    if (strcmp(name, "CUSTOMER") == 0) return CUSTOMER;
    if (strcmp(name, "EMPLOYEE") == 0) return EMPLOYEE;
    if (strcmp(name, "MANAGER") == 0) return MANAGER;
    if (strcmp(name, "ADMINISTRATOR") == 0) return ADMIN;
    return 0;
}

static int decode_record(DbTable table, int id, const char *raw, DbRecord *record) {
    char row[DB_MAX_ROW_SIZE + 1];
    char *fields[8];
    size_t size = record_size(table);
    if (size == 0 || size > DB_MAX_ROW_SIZE || raw[size - 1] != '\n') return -1;
    memcpy(row, raw, size - 1);
    row[size - 1] = '\0';
    memset(record, 0, sizeof(*record));

    if (table == DB_USERS) {
        if (split_row(row, fields, 5) != 0 || strcmp(fields[0], "USER") != 0) return -1;
        record->user.id = atoi(fields[1]);
        record->user.role = role_from_name(trim_right(fields[2]));
        if (record->user.role == 0) return -1;
        snprintf(record->user.username, sizeof(record->user.username), "%s", trim_right(fields[3]));
        snprintf(record->user.password, sizeof(record->user.password), "%s", trim_right(fields[4]));
        return record->user.id == id ? 0 : -1;
    }
    if (table == DB_ACCOUNTS) {
        if (split_row(row, fields, 4) != 0 || strcmp(fields[0], "ACCOUNT") != 0) return -1;
        record->account.user_id = atoi(fields[1]);
        record->account.balance = atoll(fields[2]);
        char *activity = trim_right(fields[3]);
        if (record->account.user_id < 1 ||
            (strcmp(activity, "ACTIVE") != 0 && strcmp(activity, "INACTIVE") != 0)) return -1;
        record->account.active = strcmp(activity, "ACTIVE") == 0;
        return 0;
    }
    if (table == DB_LOANS) {
        if (split_row(row, fields, 6) != 0 || strcmp(fields[0], "LOAN") != 0) return -1;
        record->loan.id = atoi(fields[1]);
        record->loan.customer_id = atoi(fields[2]);
        record->loan.amount = atoll(fields[3]);
        snprintf(record->loan.status, sizeof(record->loan.status), "%s", trim_right(fields[4]));
        record->loan.employee_id = atoi(fields[5]);
        return record->loan.id == id ? 0 : -1;
    }
    if (table == DB_TRANSACTIONS) {
        if (split_row(row, fields, 7) != 0 || strcmp(fields[0], "TRANSACTION") != 0) return -1;
        record->transaction.id = atoi(fields[1]);
        record->transaction.customer_id = atoi(fields[2]);
        record->transaction.amount = atoll(fields[3]);
        snprintf(record->transaction.type, sizeof(record->transaction.type), "%s", trim_right(fields[4]));
        snprintf(record->transaction.timestamp, sizeof(record->transaction.timestamp), "%s", trim_right(fields[5]));
        snprintf(record->transaction.details, sizeof(record->transaction.details), "%s", trim_right(fields[6]));
        return record->transaction.id == id ? 0 : -1;
    }
    if (table == DB_FEEDBACK) {
        if (split_row(row, fields, 4) != 0 || strcmp(fields[0], "FEEDBACK") != 0) return -1;
        record->feedback.id = atoi(fields[1]);
        record->feedback.customer_id = atoi(fields[2]);
        snprintf(record->feedback.text, sizeof(record->feedback.text), "%s", trim_right(fields[3]));
        return record->feedback.id == id ? 0 : -1;
    }
    return -1;
}

static int encode_record(DbTable table, int id, const DbRecord *record, char *row) {
    int written = -1;
    size_t size = record_size(table);
    if (size == 0 || size > DB_MAX_ROW_SIZE) return -1;
    switch (table) {
        case DB_USERS: {
            const char *stored_role = role_name(record->user.role);
            if (!stored_role || strlen(record->user.username) >= USERNAME_SIZE ||
                strlen(record->user.password) >= PASSWORD_SIZE) return -1;
            written = snprintf(row, DB_MAX_ROW_SIZE, "USER|%010d|%-13s|%-49s|%-49s",
                               id, stored_role, record->user.username, record->user.password);
            break;
        }
        case DB_ACCOUNTS:
            if (record->account.user_id < 1) return -1;
            written = snprintf(row, DB_MAX_ROW_SIZE, "ACCOUNT|%010d|%020lld|%-8s",
                               record->account.user_id, record->account.balance,
                               record->account.active ? "ACTIVE" : "INACTIVE");
            break;
        case DB_LOANS:
            if (strlen(record->loan.status) >= 16) return -1;
            written = snprintf(row, DB_MAX_ROW_SIZE, "LOAN|%010d|%010d|%020lld|%-8s|%010d",
                               id, record->loan.customer_id, record->loan.amount,
                               record->loan.status, record->loan.employee_id);
            break;
        case DB_TRANSACTIONS:
            if (strlen(record->transaction.type) >= sizeof(record->transaction.type) ||
                strlen(record->transaction.timestamp) >= sizeof(record->transaction.timestamp) ||
                strlen(record->transaction.details) >= TEXT_SIZE) return -1;
            written = snprintf(row, DB_MAX_ROW_SIZE, "TRANSACTION|%010d|%010d|%020lld|%-24s|%-19s|%-255s",
                               id, record->transaction.customer_id, record->transaction.amount,
                               record->transaction.type, record->transaction.timestamp,
                               record->transaction.details);
            break;
        case DB_FEEDBACK:
            if (strlen(record->feedback.text) >= TEXT_SIZE) return -1;
            written = snprintf(row, DB_MAX_ROW_SIZE, "FEEDBACK|%010d|%010d|%-255s",
                               id, record->feedback.customer_id, record->feedback.text);
            break;
        default:
            return -1;
    }
    if (written < 0 || (size_t)written != size - 1) return -1;
    row[size - 1] = '\n';
    return 0;
}

static uint32_t wal_checksum(const WalEntry *entries, size_t count) {
    const unsigned char *bytes = (const unsigned char *)entries;
    size_t length = count * sizeof(*entries);
    uint32_t hash = 2166136261U;
    for (size_t i = 0; i < length; i++) {
        hash ^= bytes[i];
        hash *= 16777619U;
    }
    return hash;
}

static int clear_wal(Database *database) {
    if (ftruncate(database->wal_fd, 0) != 0) return -1;
    return fsync(database->wal_fd);
}

static int replay_wal(Database *database) {
    struct stat status;
    WalHeader header;
    WalEntry entries[DB_MAX_TX_RECORDS];
    if (fstat(database->wal_fd, &status) != 0) return -1;
    if (status.st_size == 0) return 0;
    if (status.st_size < (off_t)sizeof(header)) return clear_wal(database);
    if (read_all_at(database->wal_fd, &header, sizeof(header), 0) != 0) return -1;
    if (header.magic != WAL_MAGIC || header.version != WAL_VERSION ||
        header.count == 0 || header.count > DB_MAX_TX_RECORDS) {
        if (status.st_size < (off_t)(sizeof(header) + sizeof(WalEntry)))
            return clear_wal(database);
        return -1;
    }
    off_t expected_size = (off_t)(sizeof(header) + header.count * sizeof(WalEntry));
    if (status.st_size < expected_size) return clear_wal(database);
    if (status.st_size != expected_size) return -1;
    if (read_all_at(database->wal_fd, entries, header.count * sizeof(WalEntry), sizeof(header)) != 0)
        return -1;
    if (wal_checksum(entries, header.count) != header.checksum) return -1;

    bool touched[DB_TABLE_COUNT] = { false };
    for (uint32_t i = 0; i < header.count; i++) {
        if (entries[i].table >= DB_TABLE_COUNT || entries[i].id < 1 ||
            entries[i].id > (int32_t)table_limits[entries[i].table] ||
            entries[i].size != table_row_sizes[entries[i].table]) return -1;
        off_t offset = (off_t)(entries[i].id - 1) * (off_t)entries[i].size;
        if (write_all_at(database->table_fd[entries[i].table], entries[i].row,
                         entries[i].size, offset) != 0) return -1;
        touched[entries[i].table] = true;
    }
    for (int i = 0; i < DB_TABLE_COUNT; i++)
        if (touched[i] && fsync(database->table_fd[i]) != 0) return -1;
    return clear_wal(database);
}

static int acquire_commit_lock(Database *database) {
    pthread_mutex_lock(&commit_mutex);
    if (lock_region(database->commit_fd, F_WRLCK, 0, 0) != 0) {
        pthread_mutex_unlock(&commit_mutex);
        return -1;
    }
    return 0;
}

static void release_commit_lock(Database *database) {
    lock_region(database->commit_fd, F_UNLCK, 0, 0);
    pthread_mutex_unlock(&commit_mutex);
}

static int ensure_wal_recovered(Database *database) {
    struct stat status;
    if (fstat(database->wal_fd, &status) != 0) return -1;
    if (status.st_size == 0) return 0;
    if (acquire_commit_lock(database) != 0) return -1;
    int result = replay_wal(database);
    release_commit_lock(database);
    if (result != 0) database->healthy = false;
    return result;
}

static int compact_account_rows(Database *database) {
    struct stat status;
    char table_path[PATH_MAX];
    char temporary_path[PATH_MAX];
    if (fstat(database->table_fd[DB_ACCOUNTS], &status) != 0 || status.st_size < 0)
        return -1;
    if (snprintf(table_path, sizeof(table_path), "%s/accounts.txt", database->directory) >=
        (int)sizeof(table_path) ||
        snprintf(temporary_path, sizeof(temporary_path), "%s/accounts.txt.migrate.XXXXXX",
                 database->directory) >= (int)sizeof(temporary_path)) return -1;

    off_t complete_size = status.st_size - status.st_size % ACCOUNT_ROW_SIZE;
    size_t row_count = (size_t)complete_size / ACCOUNT_ROW_SIZE;
    size_t packed_count = 0;
    bool needs_compaction = complete_size != status.st_size;
    char row[ACCOUNT_ROW_SIZE];
    for (size_t source = 1; source <= row_count; source++) {
        off_t source_offset = (off_t)(source - 1) * ACCOUNT_ROW_SIZE;
        if (read_all_at(database->table_fd[DB_ACCOUNTS], row, sizeof(row), source_offset) != 0)
            return -1;
        if (blank_account_row(row)) {
            needs_compaction = true;
            continue;
        }
        DbRecord record;
        if (decode_record(DB_ACCOUNTS, (int)source, row, &record) != 0) return -1;
        if (packed_count != source - 1) needs_compaction = true;
        packed_count++;
    }
    if (!needs_compaction) return 0;

    int temporary_fd = mkstemp(temporary_path);
    if (temporary_fd < 0) return -1;
    packed_count = 0;
    for (size_t source = 1; source <= row_count; source++) {
        off_t source_offset = (off_t)(source - 1) * ACCOUNT_ROW_SIZE;
        if (read_all_at(database->table_fd[DB_ACCOUNTS], row, sizeof(row), source_offset) != 0) {
            close(temporary_fd);
            unlink(temporary_path);
            return -1;
        }
        if (blank_account_row(row)) continue;
        off_t target_offset = (off_t)packed_count * ACCOUNT_ROW_SIZE;
        if (write_all_at(temporary_fd, row, sizeof(row), target_offset) != 0) {
            close(temporary_fd);
            unlink(temporary_path);
            return -1;
        }
        packed_count++;
    }
    int sync_result = fsync(temporary_fd);
    int close_result = close(temporary_fd);
    if (sync_result != 0 || close_result != 0 || rename(temporary_path, table_path) != 0) {
        unlink(temporary_path);
        return -1;
    }

    int replacement_fd = open(table_path, O_RDWR);
    if (replacement_fd < 0 || fchmod(replacement_fd, 0600) != 0) {
        if (replacement_fd >= 0) close(replacement_fd);
        return -1;
    }
    close(database->table_fd[DB_ACCOUNTS]);
    database->table_fd[DB_ACCOUNTS] = replacement_fd;
    return 0;
}

static int make_directory(const char *path) {
    if (mkdir(path, 0700) == 0 || errno == EEXIST) return 0;
    return -1;
}

static int open_control_file(const char *directory, const char *name) {
    char path[PATH_MAX];
    if (snprintf(path, sizeof(path), "%s/.locks/%s", directory, name) >= (int)sizeof(path))
        return -1;
    int fd = open(path, O_RDWR | O_CREAT, 0600);
    if (fd >= 0 && fchmod(fd, 0600) != 0) {
        close(fd);
        return -1;
    }
    return fd;
}

static int seed_table_record(Database *database, DbTable table, int id, const DbRecord *record) {
    char row[DB_MAX_ROW_SIZE];
    size_t size = record_size(table);
    if (encode_record(table, id, record, row) != 0) return -1;
    off_t offset = (off_t)(id - 1) * (off_t)size;
    return write_all_at(database->table_fd[table], row, size, offset);
}

static int seed_database(Database *database) {
    DbRecord record;
    memset(&record, 0, sizeof(record));

    record.user = (User){ .id = 1, .role = ADMIN, .username = "admin", .password = "admin123" };
    if (seed_table_record(database, DB_USERS, 1, &record) != 0) return -1;
    record.user = (User){ .id = 2, .role = MANAGER, .username = "manager", .password = "manager123" };
    if (seed_table_record(database, DB_USERS, 2, &record) != 0) return -1;
    record.user = (User){ .id = 3, .role = EMPLOYEE, .username = "employee", .password = "employee123" };
    if (seed_table_record(database, DB_USERS, 3, &record) != 0) return -1;
    record.user = (User){ .id = 4, .role = CUSTOMER, .username = "customer", .password = "customer123" };
    if (seed_table_record(database, DB_USERS, 4, &record) != 0) return -1;

    record.account = (Account){ .user_id = 4, .balance = 100000, .active = true };
    if (seed_table_record(database, DB_ACCOUNTS, 1, &record) != 0) return -1;

    record.transaction = (Transaction){ .id = 1, .customer_id = 4, .amount = 100000,
                                        .type = "OPENING BALANCE", .timestamp = "2026-10-24 00:00:00",
                                        .details = "Initial demo balance" };
    if (seed_table_record(database, DB_TRANSACTIONS, 1, &record) != 0) return -1;
    for (int i = 0; i < DB_TABLE_COUNT; i++)
        if (fsync(database->table_fd[i]) != 0) return -1;
    return 0;
}

Database *db_open(const char *directory, char *error, size_t error_size) {
    Database *database;
    char path[PATH_MAX];
    const char *extensions[DB_TABLE_COUNT] = {
        "users.txt", "accounts.txt", "loans.txt", "transactions.txt", "feedback.txt"
    };
    pthread_once(&lock_once, initialize_mutexes);
    if (!directory || strlen(directory) >= PATH_MAX - 32) {
        set_error(error, error_size, "Database directory path is too long.");
        return NULL;
    }
    if (make_directory(directory) != 0) {
        set_error(error, error_size, "Could not create database directory.");
        return NULL;
    }
    if (snprintf(path, sizeof(path), "%s/.locks", directory) >= (int)sizeof(path) ||
        make_directory(path) != 0) {
        set_error(error, error_size, "Could not create database lock directory.");
        return NULL;
    }

    database = calloc(1, sizeof(*database));
    if (!database) {
        set_error(error, error_size, "Not enough memory to open database.");
        return NULL;
    }
    for (int i = 0; i < DB_TABLE_COUNT; i++) {
        database->table_fd[i] = -1;
        database->append_fd[i] = -1;
    }
    database->wal_fd = database->commit_fd = database->startup_fd = -1;
    snprintf(database->directory, sizeof(database->directory), "%s", directory);
    database->healthy = true;

    for (int i = 0; i < DB_TABLE_COUNT; i++) {
        if (snprintf(path, sizeof(path), "%s/%s", directory, extensions[i]) >= (int)sizeof(path))
            goto open_failed;
        database->table_fd[i] = open(path, O_RDWR | O_CREAT, 0600);
        if (database->table_fd[i] >= 0 && fchmod(database->table_fd[i], 0600) != 0)
            goto open_failed;
        char append_name[64];
        snprintf(append_name, sizeof(append_name), "%s.append.lock", table_names[i]);
        database->append_fd[i] = open_control_file(directory, append_name);
        if (database->table_fd[i] < 0 || database->append_fd[i] < 0) goto open_failed;
    }
    database->wal_fd = open_control_file(directory, "commit.wal");
    database->commit_fd = open_control_file(directory, "commit.lock");
    database->startup_fd = open_control_file(directory, "startup.lock");
    if (database->wal_fd < 0 || database->commit_fd < 0 || database->startup_fd < 0)
        goto open_failed;

    if (lock_region(database->startup_fd, F_WRLCK, 0, 0) != 0) goto open_failed;
    if (acquire_commit_lock(database) != 0) {
        lock_region(database->startup_fd, F_UNLCK, 0, 0);
        goto open_failed;
    }
    int recovery_result = replay_wal(database);
    if (recovery_result == 0 && compact_account_rows(database) != 0) recovery_result = -1;
    size_t counts[DB_TABLE_COUNT] = {0};
    int count_result = 0;
    for (int i = 0; i < DB_TABLE_COUNT; i++)
        if (table_record_count(database, (DbTable)i, &counts[i]) != 0 ||
            counts[i] > table_limits[i]) count_result = -1;
    int seed_result = 0;
    if (recovery_result == 0 && count_result == 0 && counts[DB_USERS] == 0) {
        bool empty = true;
        for (int i = 1; i < DB_TABLE_COUNT; i++)
            if (counts[i] != 0) empty = false;
        if (empty) seed_result = seed_database(database);
        else seed_result = -1;
    }
    if (seed_result == 0) {
        for (int i = 0; i < DB_TABLE_COUNT; i++)
            if (table_record_count(database, (DbTable)i, &counts[i]) != 0 ||
                counts[i] > table_limits[i]) count_result = -1;
    }
    release_commit_lock(database);
    lock_region(database->startup_fd, F_UNLCK, 0, 0);
    if (recovery_result != 0 || count_result != 0 || seed_result != 0) goto open_failed;

    if (validate_database_relations(database) != 0) {
        set_error(error, error_size, "Database rows violate user, account, loan, or transaction relationships.");
        db_close(database);
        return NULL;
    }
    return database;

open_failed:
    set_error(error, error_size, "Could not open, initialize, or recover the text database.");
    db_close(database);
    return NULL;
}

void db_close(Database *database) {
    if (!database) return;
    for (int i = 0; i < DB_TABLE_COUNT; i++) {
        if (database->table_fd[i] >= 0) close(database->table_fd[i]);
        if (database->append_fd[i] >= 0) close(database->append_fd[i]);
    }
    if (database->wal_fd >= 0) close(database->wal_fd);
    if (database->commit_fd >= 0) close(database->commit_fd);
    if (database->startup_fd >= 0) close(database->startup_fd);
    free(database);
}

static int acquire_record_lock(Database *database, DbTable table, int id, bool write_lock,
                               int *shard) {
    *shard = row_shard(table, id);
    pthread_mutex_lock(&row_mutexes[*shard]);
    off_t offset = (off_t)(id - 1) * (off_t)record_size(table);
    if (lock_region(database->table_fd[table], write_lock ? F_WRLCK : F_RDLCK,
                    offset, (off_t)record_size(table)) != 0) {
        pthread_mutex_unlock(&row_mutexes[*shard]);
        return -1;
    }
    return 0;
}

static void release_record_lock(Database *database, DbTable table, int id, int shard) {
    off_t offset = (off_t)(id - 1) * (off_t)record_size(table);
    lock_region(database->table_fd[table], F_UNLCK, offset, (off_t)record_size(table));
    pthread_mutex_unlock(&row_mutexes[shard]);
}

static int transaction_key_after(const DbTransaction *transaction, DbTable table, int id) {
    if (!transaction->has_last_key) return 1;
    if (table > transaction->last_table) return 1;
    if (table < transaction->last_table) return 0;
    return id >= transaction->last_id;
}

DbTransaction *db_tx_begin(Database *database) {
    if (!database || !database->healthy || ensure_wal_recovered(database) != 0) return NULL;
    DbTransaction *transaction = calloc(1, sizeof(*transaction));
    if (transaction) transaction->database = database;
    return transaction;
}

int db_tx_get(DbTransaction *transaction, DbTable table, int id, bool write,
              DbRecord **record) {
    if (!transaction || !record || table < 0 || table >= DB_TABLE_COUNT || id < 1 ||
        (size_t)id > table_limits[table]) return -1;
    for (size_t i = 0; i < transaction->entry_count; i++) {
        DbEntry *entry = &transaction->entries[i];
        if (entry->table == table && entry->id == id) {
            if (write && !entry->write_lock) return -1;
            if (write) entry->dirty = true;
            *record = &entry->record;
            return 1;
        }
    }
    if (!transaction_key_after(transaction, table, id) ||
        transaction->entry_count >= DB_MAX_TX_RECORDS) return -1;

    size_t count;
    if (table_record_count(transaction->database, table, &count) != 0) return -1;
    if ((size_t)id > count) return 0;

    int shard;
    if (acquire_record_lock(transaction->database, table, id, write, &shard) != 0) return -1;
    char row[DB_MAX_ROW_SIZE];
    size_t size = record_size(table);
    off_t offset = (off_t)(id - 1) * (off_t)size;
    DbEntry *entry = &transaction->entries[transaction->entry_count];
    memset(entry, 0, sizeof(*entry));
    if (read_all_at(transaction->database->table_fd[table], row, size, offset) != 0) {
        release_record_lock(transaction->database, table, id, shard);
        return -1;
    }

    if (decode_record(table, id, row, &entry->record) != 0) {
        release_record_lock(transaction->database, table, id, shard);
        return -1;
    }
    entry->table = table;
    entry->id = id;
    entry->shard = shard;
    entry->write_lock = write;
    entry->dirty = write;
    transaction->entry_count++;
    transaction->has_last_key = true;
    transaction->last_table = table;
    transaction->last_id = id;
    *record = &entry->record;
    return 1;
}

int db_tx_reserve_append(DbTransaction *transaction, DbTable table) {
    if (!transaction || table < 0 || table >= DB_TABLE_COUNT) return -1;
    if (transaction->append[table].held) return 0;
    if ((transaction->has_last_append && table < transaction->last_append_table) ||
        (transaction->has_last_key && table < transaction->last_table)) return -1;

    pthread_mutex_lock(&append_mutexes[table]);
    if (lock_region(transaction->database->append_fd[table], F_WRLCK, 0, 0) != 0) {
        pthread_mutex_unlock(&append_mutexes[table]);
        return -1;
    }
    size_t count;
    if (table_record_count(transaction->database, table, &count) != 0 ||
        count >= table_limits[table]) {
        lock_region(transaction->database->append_fd[table], F_UNLCK, 0, 0);
        pthread_mutex_unlock(&append_mutexes[table]);
        return -1;
    }
    transaction->append[table].held = true;
    transaction->append[table].next_id = count + 1;
    transaction->has_last_append = true;
    transaction->last_append_table = table;
    return 0;
}

static void set_record_id(DbTable table, int id, DbRecord *record) {
    switch (table) {
        case DB_USERS: record->user.id = id; break;
        case DB_ACCOUNTS: break;
        case DB_LOANS: record->loan.id = id; break;
        case DB_TRANSACTIONS: record->transaction.id = id; break;
        case DB_FEEDBACK: record->feedback.id = id; break;
        default: break;
    }
}

int db_tx_append(DbTransaction *transaction, DbTable table, const DbRecord *record,
                 int *id, DbRecord **stored_record) {
    if (!transaction || !record || !id || !stored_record || table < 0 || table >= DB_TABLE_COUNT)
        return -1;
    if (!transaction->append[table].held && db_tx_reserve_append(transaction, table) != 0)
        return -1;
    if (transaction->entry_count >= DB_MAX_TX_RECORDS) return -1;
    size_t next = transaction->append[table].next_id;
    if (next > table_limits[table] || !transaction_key_after(transaction, table, (int)next)) return -1;
    int record_id = (int)next;
    int shard;
    if (acquire_record_lock(transaction->database, table, record_id, true, &shard) != 0) return -1;
    DbEntry *entry = &transaction->entries[transaction->entry_count++];
    memset(entry, 0, sizeof(*entry));
    entry->table = table;
    entry->id = record_id;
    entry->shard = shard;
    entry->write_lock = true;
    entry->dirty = true;
    entry->record = *record;
    set_record_id(table, record_id, &entry->record);
    transaction->append[table].next_id++;
    transaction->has_last_key = true;
    transaction->last_table = table;
    transaction->last_id = record_id;
    *id = record_id;
    *stored_record = &entry->record;
    return 0;
}

int db_tx_create_account(DbTransaction *transaction, int user_id, const Account *account) {
    if (!transaction || !account || user_id < 1 || account->user_id != user_id)
        return -1;
    DbRecord record = {0};
    DbRecord *stored_record = NULL;
    int record_id;
    record.account = *account;
    return db_tx_append(transaction, DB_ACCOUNTS, &record, &record_id, &stored_record);
}

static void release_transaction_locks(DbTransaction *transaction) {
    Database *database = transaction->database;
    for (size_t i = transaction->entry_count; i > 0; i--) {
        DbEntry *entry = &transaction->entries[i - 1];
        release_record_lock(database, entry->table, entry->id, entry->shard);
    }
    for (int i = DB_TABLE_COUNT - 1; i >= 0; i--) {
        if (!transaction->append[i].held) continue;
        lock_region(database->append_fd[i], F_UNLCK, 0, 0);
        pthread_mutex_unlock(&append_mutexes[i]);
    }
}

void db_tx_abort(DbTransaction *transaction) {
    if (!transaction) return;
    release_transaction_locks(transaction);
    free(transaction);
}

static int commit_transaction(DbTransaction *transaction) {
    Database *database = transaction->database;
    WalHeader header;
    WalEntry entries[DB_MAX_TX_RECORDS];
    size_t count = 0;
    bool touched[DB_TABLE_COUNT] = { false };
    memset(entries, 0, sizeof(entries));
    for (size_t i = 0; i < transaction->entry_count; i++) {
        DbEntry *entry = &transaction->entries[i];
        if (!entry->dirty) continue;
        WalEntry *wal_entry = &entries[count++];
        wal_entry->table = (uint32_t)entry->table;
        wal_entry->id = entry->id;
        wal_entry->size = (uint32_t)record_size(entry->table);
        if (encode_record(entry->table, entry->id, &entry->record, wal_entry->row) != 0)
            return -1;
    }
    if (count == 0) return 0;
    if (acquire_commit_lock(database) != 0) return -1;
    int result = -1;
    if (replay_wal(database) != 0) goto done;

    header.magic = WAL_MAGIC;
    header.version = WAL_VERSION;
    header.count = (uint32_t)count;
    header.checksum = wal_checksum(entries, count);
    if (ftruncate(database->wal_fd, 0) != 0 ||
        write_all_at(database->wal_fd, &header, sizeof(header), 0) != 0 ||
        write_all_at(database->wal_fd, entries, count * sizeof(WalEntry), sizeof(header)) != 0 ||
        fsync(database->wal_fd) != 0) {
        clear_wal(database);
        goto done;
    }

    for (size_t i = 0; i < count; i++) {
        DbTable table = (DbTable)entries[i].table;
        off_t offset = (off_t)(entries[i].id - 1) * (off_t)entries[i].size;
        if (write_all_at(database->table_fd[table], entries[i].row, entries[i].size, offset) != 0)
            goto recover;
        touched[table] = true;
    }
    for (int i = 0; i < DB_TABLE_COUNT; i++)
        if (touched[i] && fsync(database->table_fd[i]) != 0) goto recover;
    clear_wal(database);
    result = 0;
    goto done;

recover:
    if (replay_wal(database) == 0) result = 0;
    else database->healthy = false;

done:
    release_commit_lock(database);
    return result;
}

int db_tx_commit(DbTransaction *transaction) {
    if (!transaction) return -1;
    int result = commit_transaction(transaction);
    release_transaction_locks(transaction);
    free(transaction);
    return result;
}

int db_read_record(Database *database, DbTable table, int id, DbRecord *record) {
    DbTransaction *transaction = db_tx_begin(database);
    DbRecord *stored = NULL;
    if (!transaction) return -1;
    int result = db_tx_get(transaction, table, id, false, &stored);
    if (result == 1) *record = *stored;
    db_tx_abort(transaction);
    return result;
}

int db_find_user(Database *database, const char *username, User *user) {
    size_t count;
    if (db_table_count(database, DB_USERS, &count) != 0) return -1;
    for (size_t id = 1; id <= count; id++) {
        DbRecord record;
        int result = db_read_record(database, DB_USERS, (int)id, &record);
        if (result != 1) return -1;
        if (strcmp(record.user.username, username) == 0) {
            *user = record.user;
            return 1;
        }
    }
    return 0;
}

int db_find_account(Database *database, int user_id, int *record_id, Account *account) {
    if (!database || !record_id || !account || user_id < 1) return -1;
    size_t count;
    if (db_table_count(database, DB_ACCOUNTS, &count) != 0) return -1;
    bool found = false;
    Account match;
    int match_id = 0;
    for (size_t id = 1; id <= count; id++) {
        DbRecord record;
        if (db_read_record(database, DB_ACCOUNTS, (int)id, &record) != 1) return -1;
        if (record.account.user_id != user_id) continue;
        if (found) return -1;
        found = true;
        match = record.account;
        match_id = (int)id;
    }
    if (!found) return 0;
    *record_id = match_id;
    *account = match;
    return 1;
}

static int validate_database_relations(Database *database) {
    size_t user_count, account_count, loan_count, transaction_count, feedback_count;
    if (db_table_count(database, DB_USERS, &user_count) != 0 || user_count == 0 ||
        db_table_count(database, DB_ACCOUNTS, &account_count) != 0 ||
        db_table_count(database, DB_LOANS, &loan_count) != 0 ||
        db_table_count(database, DB_TRANSACTIONS, &transaction_count) != 0 ||
        db_table_count(database, DB_FEEDBACK, &feedback_count) != 0) return -1;

    int *roles = calloc(user_count + 1, sizeof(*roles));
    bool *has_account = calloc(user_count + 1, sizeof(*has_account));
    char (*usernames)[USERNAME_SIZE] = calloc(user_count + 1, sizeof(*usernames));
    if (!roles || !has_account || !usernames) {
        free(roles); free(has_account); free(usernames);
        return -1;
    }

    int result = -1;
    for (size_t id = 1; id <= user_count; id++) {
        DbRecord record;
        if (db_read_record(database, DB_USERS, (int)id, &record) != 1 ||
            record.user.id != (int)id || !record.user.username[0]) goto done;
        for (size_t previous = 1; previous < id; previous++)
            if (strcmp(usernames[previous], record.user.username) == 0) goto done;
        roles[id] = record.user.role;
        snprintf(usernames[id], USERNAME_SIZE, "%s", record.user.username);
    }

    for (size_t id = 1; id <= account_count; id++) {
        DbRecord record;
        if (db_read_record(database, DB_ACCOUNTS, (int)id, &record) != 1) goto done;
        int owner = record.account.user_id;
        if (owner < 1 || (size_t)owner > user_count || has_account[owner] ||
            record.account.balance < 0 || (record.account.active && roles[owner] != CUSTOMER)) goto done;
        has_account[owner] = true;
    }
    for (size_t id = 1; id <= user_count; id++)
        if (roles[id] == CUSTOMER && !has_account[id]) goto done;

    for (size_t id = 1; id <= loan_count; id++) {
        DbRecord record;
        if (db_read_record(database, DB_LOANS, (int)id, &record) != 1) goto done;
        Loan *loan = &record.loan;
        if (loan->customer_id < 1 || (size_t)loan->customer_id > user_count ||
            loan->amount <= 0 ||
            (loan->employee_id != 0 && (loan->employee_id < 1 ||
                                        (size_t)loan->employee_id > user_count)) ||
            (strcmp(loan->status, "PENDING") != 0 && strcmp(loan->status, "ASSIGNED") != 0 &&
             strcmp(loan->status, "APPROVED") != 0 && strcmp(loan->status, "REJECTED") != 0)) goto done;
        if ((strcmp(loan->status, "PENDING") == 0) != (loan->employee_id == 0)) goto done;
    }

    for (size_t id = 1; id <= transaction_count; id++) {
        DbRecord record;
        if (db_read_record(database, DB_TRANSACTIONS, (int)id, &record) != 1) goto done;
        Transaction *transaction = &record.transaction;
        if (transaction->customer_id < 1 || (size_t)transaction->customer_id > user_count ||
            transaction->amount <= 0) goto done;
        if (strcmp(transaction->type, "OPENING BALANCE") != 0 &&
            strcmp(transaction->type, "DEPOSIT") != 0 &&
            strcmp(transaction->type, "WITHDRAWAL") != 0 &&
            strcmp(transaction->type, "OUTGOING TRANSFER") != 0 &&
            strcmp(transaction->type, "INCOMING TRANSFER") != 0 &&
            strcmp(transaction->type, "LOAN CREDIT") != 0) goto done;
    }

    for (size_t id = 1; id <= feedback_count; id++) {
        DbRecord record;
        if (db_read_record(database, DB_FEEDBACK, (int)id, &record) != 1 ||
            record.feedback.customer_id < 1 ||
            (size_t)record.feedback.customer_id > user_count) goto done;
    }
    result = 0;

done:
    free(roles);
    free(has_account);
    free(usernames);
    return result;
}
