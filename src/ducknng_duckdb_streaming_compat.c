#include "ducknng_duckdb_streaming_compat.h"
#include "ducknng_util.h"

DUCKDB_EXTENSION_EXTERN

int ducknng_streaming_prepare_open(duckdb_prepared_statement stmt, duckdb_pending_result *out_pending, char **errmsg) {
    (void)stmt;
    (void)out_pending;
    if (errmsg) *errmsg = ducknng_strdup("ducknng streaming compat not implemented in phase 1");
    return -1;
}
int ducknng_streaming_execute_ready(duckdb_pending_result pending, duckdb_result *out_result, char **errmsg) {
    (void)pending;
    (void)out_result;
    if (errmsg) *errmsg = ducknng_strdup("ducknng streaming compat not implemented in phase 1");
    return -1;
}
duckdb_data_chunk ducknng_streaming_fetch_chunk(duckdb_result result) {
    (void)result;
    return NULL;
}
