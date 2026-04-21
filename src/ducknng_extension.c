#include "duckdb_extension.h"
#include "ducknng_runtime.h"
#include "ducknng_sql_api.h"

DUCKDB_EXTENSION_ENTRYPOINT_CUSTOM(duckdb_extension_info info, struct duckdb_extension_access *access) {
    duckdb_connection connection = NULL;
    ducknng_runtime *rt = NULL;
    duckdb_database *db = NULL;
    if (!access || !info) {
        return false;
    }
    db = access->get_database(info);
    if (!db || !*db) {
        access->set_error(info, "ducknng: failed to get database handle");
        return false;
    }
    if (duckdb_connect(*db, &connection) == DuckDBError || !connection) {
        access->set_error(info, "ducknng: failed to open init connection");
        return false;
    }
    if (!ducknng_runtime_init(connection, info, access, &rt)) {
        duckdb_disconnect(&connection);
        return false;
    }
    if (!ducknng_register_sql_api(connection, rt)) {
        access->set_error(info, "ducknng: failed to register sql api");
        duckdb_disconnect(&connection);
        return false;
    }
    return true;
}
