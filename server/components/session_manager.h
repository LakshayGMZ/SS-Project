#ifndef BANK_SESSIONS_H
#define BANK_SESSIONS_H

#include <stdbool.h>
#include <stdint.h>
#include <time.h>

typedef struct {
    int user_id;
    int role;
    time_t last_seen;
    uintptr_t connection_id;
    bool active;
} UserSession;

void sessions_init(void);
bool sessions_start(int user_id, int role, uintptr_t connection_id);
bool sessions_authorize(int user_id, int role, uintptr_t connection_id);
void sessions_logout(int user_id);
void sessions_logout_connection(uintptr_t connection_id);
void sessions_invalidate(int user_id);

#endif
