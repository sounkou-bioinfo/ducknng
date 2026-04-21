#pragma once
#include "duckdb_extension.h"
#include "ducknng_service.h"
#include "ducknng_thread.h"
#include "ducknng_registry.h"
#include <stddef.h>
#include <stdint.h>

typedef struct ducknng_runtime {
    duckdb_database *db;
    duckdb_connection init_con;
    ducknng_mutex mu;
    ducknng_service **services;
    size_t service_count;
    size_t service_cap;
    uint64_t next_service_id;
    int shutting_down;
    ducknng_method_registry registry;
} ducknng_runtime;

int ducknng_runtime_init(duckdb_connection connection, duckdb_extension_info info,
    struct duckdb_extension_access *access, ducknng_runtime **out_rt);
void ducknng_runtime_destroy(ducknng_runtime *rt);
ducknng_service *ducknng_runtime_find_service(ducknng_runtime *rt, const char *name);
int ducknng_runtime_add_service(ducknng_runtime *rt, ducknng_service *svc, char **errmsg);
ducknng_service *ducknng_runtime_remove_service(ducknng_runtime *rt, const char *name);
