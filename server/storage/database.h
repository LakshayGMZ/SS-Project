#ifndef BANK_DATABASE_H
#define BANK_DATABASE_H

#include "../utils/common.h"
#include <stdint.h>

typedef enum {
    DB_USERS,
    DB_ACCOUNTS,
    DB_LOANS,
    DB_TRANSACTIONS,
    DB_FEEDBACK,
    DB_TABLE_COUNT
} DbTable;

typedef union {
    User user;
    Account account;
    Loan loan;
    Transaction transaction;
    Feedback feedback;
} DbRecord;

typedef struct Database Database;
typedef struct DbTransaction DbTransaction;

Database *db_open(const char *directory, char *error, size_t error_size);
void db_close(Database *database);

size_t db_table_limit(DbTable table);
int db_table_count(Database *database, DbTable table, size_t *count);
int db_read_record(Database *database, DbTable table, int id, DbRecord *record);
int db_find_user(Database *database, const char *username, User *user);
int db_find_account(Database *database, int user_id, int *record_id, Account *account);

DbTransaction *db_tx_begin(Database *database);
int db_tx_get(DbTransaction *transaction, DbTable table, int id, bool write,
              DbRecord **record);
int db_tx_reserve_append(DbTransaction *transaction, DbTable table);
int db_tx_append(DbTransaction *transaction, DbTable table, const DbRecord *record,
                 int *id, DbRecord **stored_record);
int db_tx_create_account(DbTransaction *transaction, int user_id, const Account *account);
int db_tx_commit(DbTransaction *transaction);
void db_tx_abort(DbTransaction *transaction);

#endif
