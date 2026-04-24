#pragma once
#include "duckdb_extension.h"
#include "ducknng_thread.h"
#include <stdint.h>

typedef struct ducknng_schema_cache {
    idx_t ncols;
    char **names;
    duckdb_logical_type *types;
} ducknng_schema_cache;

typedef struct ducknng_session {
    uint64_t session_id;
    duckdb_result result;
    int result_open;
    int eos;
    int cancelled;
    uint64_t batch_no;
    uint64_t last_touch_ms;
    ducknng_mutex mu;
    ducknng_cond cv;
    uint32_t refcount;
    int closing;
    int mu_initialized;
    int cv_initialized;
} ducknng_session;

typedef struct ducknng_service ducknng_service;

ducknng_session *ducknng_session_create(duckdb_result *result, uint64_t session_id, char **errmsg);
void ducknng_session_destroy(ducknng_session *session);
void ducknng_session_release(ducknng_session *session);
int ducknng_service_add_session(ducknng_service *svc, duckdb_result *result, uint64_t *out_session_id, char **errmsg);
ducknng_session *ducknng_service_acquire_session(ducknng_service *svc, uint64_t session_id);
ducknng_session *ducknng_service_remove_session(ducknng_service *svc, uint64_t session_id);
ducknng_session **ducknng_service_detach_all_sessions(ducknng_service *svc, size_t *out_count);
size_t ducknng_service_prune_idle_sessions(ducknng_service *svc, uint64_t now_ms);
