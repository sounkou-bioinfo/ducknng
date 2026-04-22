#pragma once
#include "duckdb_extension.h"
#include "nanoarrow/nanoarrow.h"

int ducknng_streaming_prepare_open(duckdb_prepared_statement stmt, duckdb_pending_result *out_pending, char **errmsg);
int ducknng_streaming_execute_ready(duckdb_pending_result pending, duckdb_result *out_result, char **errmsg);
duckdb_data_chunk ducknng_streaming_fetch_chunk(duckdb_result result);

int ducknng_arrow_query_open(duckdb_connection con, const char *sql, duckdb_arrow *out_result, char **errmsg);
int ducknng_arrow_query_take_schema(duckdb_arrow result, struct ArrowSchema *out_schema, char **errmsg);
int ducknng_arrow_query_next_array(duckdb_arrow result, struct ArrowArray *out_array, int *has_array, char **errmsg);
void ducknng_arrow_query_close(duckdb_arrow *result);
