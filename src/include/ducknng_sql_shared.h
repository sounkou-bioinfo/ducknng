#pragma once
#include "duckdb_extension.h"
#include "ducknng_runtime.h"

typedef struct {
    ducknng_runtime *rt;
    int is_init_connection;
} ducknng_sql_context;

int ducknng_register_sql_auth(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_monitor(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_service(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_tls(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_socket(duckdb_connection con, ducknng_sql_context *ctx);
int ducknng_register_sql_aio(duckdb_connection con, ducknng_sql_context *ctx);
