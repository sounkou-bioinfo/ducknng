#include "ducknng_manifest.h"
#include "ducknng_ipc_in.h"
#include "ducknng_ipc_out.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_method_manifest_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    char *json = NULL;
    size_t payload_len;
    uint8_t *payload;
    char *errmsg = NULL;
    (void)method;
    if (!svc || !svc->rt || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing runtime for manifest request");
        return -1;
    }
    if (req && req->frame && req->frame->payload_len != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            "ducknng: manifest request does not accept a payload");
        return -1;
    }
    json = ducknng_method_registry_manifest_json(&svc->rt->registry, "ducknng", "0.1.0",
        DUCKNNG_WIRE_VERSION, &errmsg);
    if (!json) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            errmsg ? errmsg : "ducknng: failed to generate manifest json");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }
    payload_len = strlen(json);
    payload = (uint8_t *)duckdb_malloc(payload_len);
    if (!payload) {
        duckdb_free(json);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: out of memory preparing manifest reply");
        return -1;
    }
    memcpy(payload, json, payload_len);
    duckdb_free(json);
    ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
        DUCKNNG_RPC_FLAG_PAYLOAD_JSON, payload, payload_len);
    return 0;
}

static int ducknng_method_exec_handler(ducknng_service *svc,
    const ducknng_method_descriptor *method,
    const ducknng_request_context *req,
    ducknng_method_reply *reply) {
    ducknng_exec_request exec_req;
    duckdb_prepared_statement stmt = NULL;
    duckdb_result result;
    duckdb_statement_type stmt_type;
    duckdb_result_type result_type;
    idx_t rows_changed;
    uint8_t *payload = NULL;
    size_t payload_len = 0;
    char *errmsg = NULL;
    (void)method;

    memset(&exec_req, 0, sizeof(exec_req));
    memset(&result, 0, sizeof(result));
    if (!svc || !svc->rt || !svc->rt->init_con || !req || !req->frame || !reply) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing execution context");
        return -1;
    }
    if (ducknng_decode_exec_request_payload(req->frame->payload, (size_t)req->frame->payload_len,
            &exec_req, &errmsg) != 0) {
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_INVALID,
            errmsg ? errmsg : "ducknng: invalid exec payload");
        if (errmsg) duckdb_free(errmsg);
        return -1;
    }

    ducknng_mutex_lock(&svc->mu);
    if (duckdb_prepare(svc->rt->init_con, exec_req.sql, &stmt) == DuckDBError) {
        const char *prep_err = stmt ? duckdb_prepare_error(stmt) : NULL;
        ducknng_mutex_unlock(&svc->mu);
        if (stmt) duckdb_destroy_prepare(&stmt);
        ducknng_exec_request_destroy(&exec_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            prep_err && prep_err[0] ? prep_err : "ducknng: failed to prepare exec request");
        return -1;
    }

    stmt_type = duckdb_prepared_statement_type(stmt);
    if (duckdb_execute_prepared(stmt, &result) == DuckDBError) {
        const char *exec_err = duckdb_result_error(&result);
        duckdb_destroy_result(&result);
        duckdb_destroy_prepare(&stmt);
        ducknng_mutex_unlock(&svc->mu);
        ducknng_exec_request_destroy(&exec_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            exec_err && exec_err[0] ? exec_err : "ducknng: exec failed");
        return -1;
    }

    result_type = duckdb_result_return_type(result);
    if (!exec_req.want_result && result_type == DUCKDB_RESULT_TYPE_QUERY_RESULT) {
        duckdb_destroy_result(&result);
        duckdb_destroy_prepare(&stmt);
        ducknng_mutex_unlock(&svc->mu);
        ducknng_exec_request_destroy(&exec_req);
        ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_SQL_ERROR,
            "ducknng: EXEC result requires want_result = true");
        return -1;
    }

    if (result_type == DUCKDB_RESULT_TYPE_QUERY_RESULT) {
        if (ducknng_result_to_ipc_stream(stmt, result, &payload, &payload_len, &errmsg) != 0) {
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
                errmsg ? errmsg : "ducknng: failed to encode query result as Arrow IPC");
        } else {
            ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
                DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
                payload, payload_len);
            payload = NULL;
        }
    } else {
        rows_changed = duckdb_rows_changed(&result);
        duckdb_destroy_result(&result);
        memset(&result, 0, sizeof(result));
        if (ducknng_exec_metadata_to_ipc((uint64_t)rows_changed,
                (uint32_t)stmt_type, (uint32_t)result_type, &payload, &payload_len, &errmsg) != 0) {
            ducknng_method_reply_set_error(reply, DUCKNNG_STATUS_ARROW_ERROR,
                errmsg ? errmsg : "ducknng: failed to encode exec metadata as Arrow IPC");
        } else {
            ducknng_method_reply_set_payload(reply, DUCKNNG_RPC_RESULT,
                DUCKNNG_RPC_FLAG_RESULT_METADATA | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
                payload, payload_len);
            payload = NULL;
        }
    }

    duckdb_destroy_result(&result);
    duckdb_destroy_prepare(&stmt);
    ducknng_mutex_unlock(&svc->mu);
    if (payload) duckdb_free(payload);
    if (errmsg) duckdb_free(errmsg);
    ducknng_exec_request_destroy(&exec_req);
    return reply->type == DUCKNNG_RPC_ERROR ? -1 : 0;
}

const ducknng_method_descriptor ducknng_method_exec = {
    "exec",
    "sql",
    "Execute SQL and return metadata or rows",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_PAYLOAD_ARROW_IPC_STREAM,
    DUCKNNG_RESPONSE_METADATA_OR_ROWS,
    DUCKNNG_SESSION_STATELESS,
    0,
    DUCKNNG_RPC_FLAG_RESULT_ROWS | DUCKNNG_RPC_FLAG_RESULT_METADATA | DUCKNNG_RPC_FLAG_PAYLOAD_ARROW_STREAM,
    16 * 1024 * 1024,
    16 * 1024 * 1024,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    0,
    1,
    "{\"fields\":[{\"name\":\"sql\",\"type\":\"utf8\",\"nullable\":false},{\"name\":\"want_result\",\"type\":\"bool\",\"nullable\":false}]}",
    "{\"mode\":\"metadata_or_rows\"}",
    ducknng_method_exec_handler
};

const ducknng_method_descriptor ducknng_method_manifest = {
    "manifest",
    "control",
    "Return the registry-derived manifest JSON",
    DUCKNNG_TRANSPORT_REQREP,
    DUCKNNG_PAYLOAD_NONE,
    DUCKNNG_PAYLOAD_JSON,
    DUCKNNG_RESPONSE_METADATA_ONLY,
    DUCKNNG_SESSION_STATELESS,
    0,
    DUCKNNG_RPC_FLAG_PAYLOAD_JSON,
    0,
    1024 * 1024,
    0,
    0,
    0,
    0,
    0,
    1,
    0,
    0,
    1,
    NULL,
    "{\"type\":\"json\"}",
    ducknng_method_manifest_handler
};

int ducknng_register_builtin_methods(ducknng_runtime *rt, char **errmsg) {
    const ducknng_method_descriptor *methods[] = {
        &ducknng_method_manifest,
        &ducknng_method_exec
    };
    if (!rt) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing runtime for method registration");
        return 0;
    }
    return ducknng_method_registry_register_many(&rt->registry, methods,
        sizeof(methods) / sizeof(methods[0]), errmsg);
}
