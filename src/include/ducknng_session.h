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
    duckdb_connection con;
    duckdb_prepared_statement stmt;
    duckdb_pending_result pending;
    duckdb_result result;
    int stmt_open;
    int pending_open;
    int result_open;
    int eos;
    char *input_table_name;
    ducknng_schema_cache schema;
    uint64_t batch_no;
    uint64_t last_touch_ms;
    ducknng_mutex mu;
} ducknng_session;
