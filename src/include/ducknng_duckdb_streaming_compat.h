#pragma once
#include "duckdb_extension.h"

int ducknng_streaming_prepare_open(duckdb_prepared_statement stmt, duckdb_pending_result *out_pending, char **errmsg);
int ducknng_streaming_execute_ready(duckdb_pending_result pending, duckdb_result *out_result, char **errmsg);
duckdb_data_chunk ducknng_streaming_fetch_chunk(duckdb_result result);
