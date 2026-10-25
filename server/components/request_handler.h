#ifndef BANK_HANDLERS_H
#define BANK_HANDLERS_H

#include "../storage/database.h"
#include <stdint.h>

void handle_request(Database *database, char *request, char *response,
                    uintptr_t connection_id);

#endif
