#define _POSIX_C_SOURCE 200809L
#include "session_manager.h"
#include "../utils/common.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define MAX_SESSIONS 2000
#define SESSION_RECORD_SIZE 512
#define SESSION_HOST_SIZE 256

typedef struct {
    int user_id;
    int role;
    time_t last_seen;
    uintptr_t connection_id;
    bool active;
} LocalSession;

typedef struct {
    long long process_id;
    int role;
    unsigned long long connection_id;
    long long last_seen;
    char host[SESSION_HOST_SIZE];
} StoredSession;

static LocalSession sessions[MAX_SESSIONS];
static pthread_mutex_t session_mutex = PTHREAD_MUTEX_INITIALIZER;
static char sessions_directory[PATH_MAX];
static char locks_directory[PATH_MAX];
static char local_hostname[SESSION_HOST_SIZE];
static bool initialized;

static int make_private_directory(const char *path) {
    struct stat status;
    if (mkdir(path, 0700) != 0 && errno != EEXIST) return -1;
    if (stat(path, &status) != 0 || !S_ISDIR(status.st_mode)) return -1;
    return chmod(path, 0700);
}

static int lock_region(int fd, short type) {
    struct flock lock = {
        .l_type = type,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0
    };
    int command = type == F_UNLCK ? F_SETLK : F_SETLKW;
    while (fcntl(fd, command, &lock) < 0) {
        if (errno == EINTR && type != F_UNLCK) continue;
        return -1;
    }
    return 0;
}

static int make_paths(int user_id, char *session_path, size_t session_size,
                      char *lock_path, size_t lock_size) {
    int session_length = snprintf(session_path, session_size, "%s/session_%d.txt",
                                  sessions_directory, user_id);
    int lock_length = snprintf(lock_path, lock_size, "%s/session_%d.lock",
                               locks_directory, user_id);
    return session_length < 0 || (size_t)session_length >= session_size ||
           lock_length < 0 || (size_t)lock_length >= lock_size ? -1 : 0;
}

static int acquire_user_lock(int user_id, int *fd_out) {
    char session_path[PATH_MAX], lock_path[PATH_MAX];
    if (!fd_out || make_paths(user_id, session_path, sizeof(session_path),
                              lock_path, sizeof(lock_path)) != 0) return -1;
    int fd = open(lock_path, O_RDWR | O_CREAT, 0600);
    if (fd < 0) return -1;
    if (fchmod(fd, 0600) != 0 || lock_region(fd, F_WRLCK) != 0) {
        close(fd);
        return -1;
    }
    *fd_out = fd;
    return 0;
}

static void release_user_lock(int fd) {
    if (fd >= 0) {
        lock_region(fd, F_UNLCK);
        close(fd);
    }
}

static int read_session_file(int user_id, StoredSession *stored) {
    char session_path[PATH_MAX], lock_path[PATH_MAX];
    char buffer[SESSION_RECORD_SIZE];
    if (make_paths(user_id, session_path, sizeof(session_path), lock_path, sizeof(lock_path)) != 0)
        return -1;
    int fd = open(session_path, O_RDONLY);
    if (fd < 0) return errno == ENOENT ? 0 : -1;

    size_t used = 0;
    while (used < sizeof(buffer) - 1) {
        ssize_t result = read(fd, buffer + used, sizeof(buffer) - 1 - used);
        if (result < 0 && errno == EINTR) continue;
        if (result < 0) { close(fd); return -1; }
        if (result == 0) break;
        used += (size_t)result;
    }
    char extra;
    ssize_t extra_result = read(fd, &extra, 1);
    int close_result = close(fd);
    if (extra_result != 0 || close_result != 0 || used == 0) return -1;
    buffer[used] = '\0';

    int consumed = 0;
    int fields = sscanf(buffer, "%lld %d %llu %lld %255s %n",
                        &stored->process_id, &stored->role, &stored->connection_id,
                        &stored->last_seen, stored->host, &consumed);
    if (fields != 5 || consumed <= 0) return -1;
    for (size_t i = (size_t)consumed; i < used; i++)
        if (buffer[i] != '\n' && buffer[i] != '\r' && buffer[i] != ' ' && buffer[i] != '\t')
            return -1;
    if (stored->process_id < 1 || stored->role < CUSTOMER || stored->role > ADMIN ||
        stored->last_seen < 0 || !stored->host[0]) return -1;
    return 1;
}

static int write_all(int fd, const char *buffer, size_t size) {
    size_t written = 0;
    while (written < size) {
        ssize_t result = write(fd, buffer + written, size - written);
        if (result < 0 && errno == EINTR) continue;
        if (result <= 0) return -1;
        written += (size_t)result;
    }
    return 0;
}

static int write_session_file(int user_id, int role, uintptr_t connection_id, time_t last_seen) {
    char final_path[PATH_MAX], lock_path[PATH_MAX], temporary_path[PATH_MAX];
    if (make_paths(user_id, final_path, sizeof(final_path), lock_path, sizeof(lock_path)) != 0)
        return -1;
    int length = snprintf(temporary_path, sizeof(temporary_path), "%s/.session_%d.XXXXXX",
                          sessions_directory, user_id);
    if (length < 0 || (size_t)length >= sizeof(temporary_path)) return -1;

    int fd = mkstemp(temporary_path);
    if (fd < 0) return -1;
    int result = -1;
    char record[SESSION_RECORD_SIZE];
    int record_length = snprintf(record, sizeof(record), "%ld %d %llu %lld %s\n",
                                 (long)getpid(), role,
                                 (unsigned long long)connection_id,
                                 (long long)last_seen, local_hostname);
    if (record_length > 0 && (size_t)record_length < sizeof(record) &&
        fchmod(fd, 0600) == 0 &&
        write_all(fd, record, (size_t)record_length) == 0 && close(fd) == 0) {
        fd = -1;
        if (rename(temporary_path, final_path) == 0) result = 0;
    }
    if (fd >= 0) close(fd);
    if (result != 0) unlink(temporary_path);
    return result;
}

static bool session_is_expired(time_t now, long long last_seen) {
    return last_seen <= 0 || ((long long)now >= last_seen &&
                              (long long)now - last_seen > SESSION_TIMEOUT);
}

static bool process_is_alive(const StoredSession *stored) {
    if (strcmp(stored->host, local_hostname) != 0) return true;
    if (stored->process_id > INT_MAX) return false;
    if (kill((pid_t)stored->process_id, 0) == 0 || errno == EPERM) return true;
    return errno != ESRCH;
}

static bool stored_matches_local(const StoredSession *stored, const LocalSession *local) {
    return stored->process_id == (long long)getpid() && stored->role == local->role &&
           stored->connection_id == (unsigned long long)local->connection_id &&
           strcmp(stored->host, local_hostname) == 0;
}

static void remove_session_file(int user_id, const LocalSession *expected, bool require_match) {
    int lock_fd = -1;
    if (acquire_user_lock(user_id, &lock_fd) != 0) return;
    StoredSession stored;
    int read_result = read_session_file(user_id, &stored);
    if (read_result == 1 && (!require_match || stored_matches_local(&stored, expected))) {
        char session_path[PATH_MAX], lock_path[PATH_MAX];
        if (make_paths(user_id, session_path, sizeof(session_path), lock_path, sizeof(lock_path)) == 0)
            unlink(session_path);
    }
    release_user_lock(lock_fd);
}

static void expire_local_sessions(time_t now) {
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        LocalSession *session = &sessions[i];
        if (!session->active || now - session->last_seen <= SESSION_TIMEOUT) continue;
        remove_session_file(session->user_id, session, true);
        session->active = false;
    }
}

bool sessions_init(const char *database_directory) {
    char sessions_path[PATH_MAX], locks_path[PATH_MAX];
    if (!database_directory || strlen(database_directory) >= PATH_MAX - 16) return false;
    if (snprintf(sessions_path, sizeof(sessions_path), "%s/sessions", database_directory) >=
            (int)sizeof(sessions_path) ||
        snprintf(locks_path, sizeof(locks_path), "%s/.locks", database_directory) >=
            (int)sizeof(locks_path) ||
        make_private_directory(sessions_path) != 0 || make_private_directory(locks_path) != 0)
        return false;

    char hostname[SESSION_HOST_SIZE];
    if (gethostname(hostname, sizeof(hostname)) != 0) return false;
    hostname[sizeof(hostname) - 1] = '\0';
    if (!hostname[0]) return false;

    pthread_mutex_lock(&session_mutex);
    memset(sessions, 0, sizeof(sessions));
    snprintf(sessions_directory, sizeof(sessions_directory), "%s", sessions_path);
    snprintf(locks_directory, sizeof(locks_directory), "%s", locks_path);
    snprintf(local_hostname, sizeof(local_hostname), "%s", hostname);
    initialized = true;
    pthread_mutex_unlock(&session_mutex);
    return true;
}

bool sessions_start(int user_id, int role, uintptr_t connection_id) {
    if (user_id < 1 || role < CUSTOMER || role > ADMIN) return false;
    time_t now = time(NULL);
    pthread_mutex_lock(&session_mutex);
    if (!initialized) { pthread_mutex_unlock(&session_mutex); return false; }
    expire_local_sessions(now);

    size_t free_slot = MAX_SESSIONS;
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        if (sessions[i].active && sessions[i].user_id == user_id) {
            pthread_mutex_unlock(&session_mutex);
            return false;
        }
        if (!sessions[i].active && free_slot == MAX_SESSIONS) free_slot = i;
    }
    if (free_slot == MAX_SESSIONS) { pthread_mutex_unlock(&session_mutex); return false; }

    int lock_fd = -1;
    if (acquire_user_lock(user_id, &lock_fd) != 0) {
        pthread_mutex_unlock(&session_mutex);
        return false;
    }
    StoredSession stored;
    int read_result = read_session_file(user_id, &stored);
    if (read_result < 0 ||
        (read_result == 1 && !session_is_expired(now, stored.last_seen) &&
         process_is_alive(&stored))) {
        release_user_lock(lock_fd);
        pthread_mutex_unlock(&session_mutex);
        return false;
    }
    int write_result = write_session_file(user_id, role, connection_id, now);
    release_user_lock(lock_fd);
    if (write_result != 0) {
        pthread_mutex_unlock(&session_mutex);
        return false;
    }

    sessions[free_slot] = (LocalSession){ user_id, role, now, connection_id, true };
    pthread_mutex_unlock(&session_mutex);
    return true;
}

bool sessions_authorize(int user_id, int role, uintptr_t connection_id) {
    time_t now = time(NULL);
    bool authorized = false;
    pthread_mutex_lock(&session_mutex);
    if (!initialized) { pthread_mutex_unlock(&session_mutex); return false; }
    expire_local_sessions(now);

    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        LocalSession *local = &sessions[i];
        if (!local->active || local->user_id != user_id || local->role != role ||
            local->connection_id != connection_id) continue;

        int lock_fd = -1;
        if (acquire_user_lock(user_id, &lock_fd) != 0) break;
        StoredSession stored;
        int read_result = read_session_file(user_id, &stored);
        bool matches = read_result == 1 && stored_matches_local(&stored, local) &&
                      !session_is_expired(now, stored.last_seen) && process_is_alive(&stored);
        if (matches && write_session_file(user_id, role, connection_id, now) == 0) {
            local->last_seen = now;
            authorized = true;
        } else {
            if (read_result == 1 && stored_matches_local(&stored, local)) {
                char session_path[PATH_MAX], lock_path[PATH_MAX];
                if (make_paths(user_id, session_path, sizeof(session_path), lock_path,
                               sizeof(lock_path)) == 0) unlink(session_path);
            }
            local->active = false;
        }
        release_user_lock(lock_fd);
        break;
    }
    pthread_mutex_unlock(&session_mutex);
    return authorized;
}

void sessions_logout(int user_id) {
    if (user_id < 1) return;
    pthread_mutex_lock(&session_mutex);
    if (initialized) remove_session_file(user_id, NULL, false);
    for (size_t i = 0; i < MAX_SESSIONS; i++)
        if (sessions[i].active && sessions[i].user_id == user_id)
            sessions[i].active = false;
    pthread_mutex_unlock(&session_mutex);
}

void sessions_logout_connection(uintptr_t connection_id) {
    pthread_mutex_lock(&session_mutex);
    for (size_t i = 0; i < MAX_SESSIONS; i++) {
        LocalSession *session = &sessions[i];
        if (!session->active || session->connection_id != connection_id) continue;
        remove_session_file(session->user_id, session, true);
        session->active = false;
    }
    pthread_mutex_unlock(&session_mutex);
}

void sessions_invalidate(int user_id) {
    sessions_logout(user_id);
}
