#include "ducknng_duckdb_streaming_compat.h"
#include "ducknng_util.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_take_arrow_schema(duckdb_arrow_schema wrapper, struct ArrowSchema *out_schema, char **errmsg) {
    struct ArrowSchema *schema;
    if (!wrapper || !wrapper->internal_ptr || !out_schema) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing DuckDB Arrow schema wrapper");
        return -1;
    }
    schema = (struct ArrowSchema *)wrapper->internal_ptr;
    *out_schema = *schema;
    memset(schema, 0, sizeof(*schema));
    return 0;
}

static int ducknng_take_arrow_array(duckdb_arrow_array wrapper, struct ArrowArray *out_array, int *has_array, char **errmsg) {
    struct ArrowArray *array;
    if (has_array) *has_array = 0;
    if (!out_array) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing DuckDB Arrow array output");
        return -1;
    }
    memset(out_array, 0, sizeof(*out_array));
    if (!wrapper || !wrapper->internal_ptr) return 0;
    array = (struct ArrowArray *)wrapper->internal_ptr;
    *out_array = *array;
    memset(array, 0, sizeof(*array));
    if (has_array) *has_array = out_array->release != NULL;
    return 0;
}

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

int ducknng_arrow_query_open(duckdb_connection con, const char *sql, duckdb_arrow *out_result, char **errmsg) {
    duckdb_arrow result = NULL;
    if (!con || !sql || !sql[0] || !out_result) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: arrow query open requires connection and sql");
        return -1;
    }
    if (duckdb_query_arrow(con, sql, &result) == DuckDBError) {
        const char *detail = result ? duckdb_query_arrow_error(result) : NULL;
        if (errmsg) *errmsg = ducknng_strdup(detail && detail[0] ? detail : "ducknng: duckdb_query_arrow failed");
        if (result) duckdb_destroy_arrow(&result);
        return -1;
    }
    *out_result = result;
    return 0;
}

int ducknng_arrow_query_take_schema(duckdb_arrow result, struct ArrowSchema *out_schema, char **errmsg) {
    duckdb_arrow_schema wrapper = NULL;
    if (!result || !out_schema) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing arrow result for schema export");
        return -1;
    }
    memset(out_schema, 0, sizeof(*out_schema));
    if (duckdb_query_arrow_schema(result, &wrapper) == DuckDBError) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to obtain DuckDB Arrow schema");
        return -1;
    }
    return ducknng_take_arrow_schema(wrapper, out_schema, errmsg);
}

int ducknng_arrow_query_next_array(duckdb_arrow result, struct ArrowArray *out_array, int *has_array, char **errmsg) {
    duckdb_arrow_array wrapper = NULL;
    if (!result || !out_array) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing arrow result for batch export");
        return -1;
    }
    if (duckdb_query_arrow_array(result, &wrapper) == DuckDBError) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to obtain DuckDB Arrow batch");
        return -1;
    }
    return ducknng_take_arrow_array(wrapper, out_array, has_array, errmsg);
}

void ducknng_arrow_query_close(duckdb_arrow *result) {
    if (!result || !*result) return;
    duckdb_destroy_arrow(result);
}
