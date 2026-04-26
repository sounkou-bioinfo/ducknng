#include "ducknng_sql_api.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_shared.h"

DUCKDB_EXTENSION_EXTERN

int ducknng_register_sql_api(duckdb_connection connection, ducknng_runtime *rt) {
    ducknng_sql_context ctx;
    ctx.rt = rt;
    ctx.is_init_connection = rt && connection == rt->init_con;
    if (!ducknng_register_sql_service(connection, &ctx)) return 0;
    if (!ducknng_register_sql_auth(connection, &ctx)) return 0;
    if (!ducknng_register_sql_monitor(connection, &ctx)) return 0;
    if (!ducknng_register_sql_tls(connection, &ctx)) return 0;
    if (!ducknng_register_sql_socket(connection, &ctx)) return 0;
    if (!ducknng_register_sql_rpc(connection, &ctx)) return 0;
    if (!ducknng_register_sql_aio(connection, &ctx)) return 0;
    if (!ducknng_register_sql_registry(connection, &ctx)) return 0;
    if (!ducknng_register_sql_session(connection, &ctx)) return 0;
    if (!ducknng_register_sql_body(connection, &ctx)) return 0;
    return 1;
}
