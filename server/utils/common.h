#ifndef BANK_COMMON_H
#define BANK_COMMON_H

#include <stdbool.h>
#include <stddef.h>
#include <time.h>

#define DEFAULT_PORT 8080
#define REQUEST_SIZE 2048
#define RESPONSE_SIZE 8192
#define USERNAME_SIZE 50
#define PASSWORD_SIZE 50
#define TEXT_SIZE 256
#define SESSION_TIMEOUT 1800

enum Role {
    CUSTOMER = 1,
    EMPLOYEE = 2,
    MANAGER = 3,
    ADMIN = 4
};

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
    char type[24];
    char details[TEXT_SIZE];
    char timestamp[20];
} Transaction;

typedef struct {
    int id;
    int customer_id;
    char text[TEXT_SIZE];
} Feedback;

#endif
