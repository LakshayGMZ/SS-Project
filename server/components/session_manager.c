#define _POSIX_C_SOURCE 200809L
#include "session_manager.h"
#include "../utils/common.h"

#include <pthread.h>
#include <string.h>
#include <time.h>

#define MAX_SESSIONS 2000

static UserSession sessions[MAX_SESSIONS];
static pthread_mutex_t session_mutex = PTHREAD_MUTEX_INITIALIZER;

static void expire_sessions(time_t now) {
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        if (sessions[i].active && now - sessions[i].last_seen > SESSION_TIMEOUT)
            sessions[i].active = false;
    }
}

void sessions_init(void) {
    pthread_mutex_lock(&session_mutex);
    memset(sessions, 0, sizeof(sessions));
    pthread_mutex_unlock(&session_mutex);
}

bool sessions_start(int user_id, int role) {
    time_t now = time(NULL);
    pthread_mutex_lock(&session_mutex);
    expire_sessions(now);
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        if (sessions[i].active && sessions[i].user_id == user_id) {
            pthread_mutex_unlock(&session_mutex);
            return false;
        }
    }
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        if (!sessions[i].active) {
            sessions[i] = (UserSession){ user_id, role, now, true };
            pthread_mutex_unlock(&session_mutex);
            return true;
        }
    }
    pthread_mutex_unlock(&session_mutex);
    return false;
}

bool sessions_authorize(int user_id, int role) {
    time_t now = time(NULL);
    bool authorized = false;
    pthread_mutex_lock(&session_mutex);
    expire_sessions(now);
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        if (sessions[i].active && sessions[i].user_id == user_id && sessions[i].role == role) {
            sessions[i].last_seen = now;
            authorized = true;
            break;
        }
    }
    pthread_mutex_unlock(&session_mutex);
    return authorized;
}

void sessions_logout(int user_id) {
    pthread_mutex_lock(&session_mutex);
    for (size_t i = 0; i < MAX_SESSIONS; i++)
        if (sessions[i].active && sessions[i].user_id == user_id)
            sessions[i].active = false;
    pthread_mutex_unlock(&session_mutex);
}

void sessions_invalidate(int user_id) {
    sessions_logout(user_id);
}
