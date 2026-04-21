#include "ducknng_registry.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static int ducknng_registry_reserve(ducknng_method_registry *registry, size_t want, char **errmsg) {
    const ducknng_method_descriptor **next_methods;
    size_t next_cap = registry->method_cap ? registry->method_cap * 2 : 8;
    if (registry->method_cap >= want) return 1;
    while (next_cap < want) next_cap *= 2;
    next_methods = (const ducknng_method_descriptor **)duckdb_malloc(sizeof(*next_methods) * next_cap);
    if (!next_methods) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing method registry");
        return 0;
    }
    memset(next_methods, 0, sizeof(*next_methods) * next_cap);
    if (registry->methods && registry->method_count) {
        memcpy(next_methods, registry->methods, sizeof(*next_methods) * registry->method_count);
        duckdb_free(registry->methods);
    }
    registry->methods = next_methods;
    registry->method_cap = next_cap;
    return 1;
}

static int ducknng_method_name_exists(const ducknng_method_registry *registry, const char *name) {
    size_t i;
    if (!registry || !name) return 0;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        if (method && method->name && strcmp(method->name, name) == 0) return 1;
    }
    return 0;
}

int ducknng_method_registry_init(ducknng_method_registry *registry) {
    if (!registry) return 0;
    memset(registry, 0, sizeof(*registry));
    return 1;
}

void ducknng_method_registry_destroy(ducknng_method_registry *registry) {
    if (!registry) return;
    if (registry->methods) duckdb_free(registry->methods);
    memset(registry, 0, sizeof(*registry));
}

int ducknng_method_registry_register(ducknng_method_registry *registry,
    const ducknng_method_descriptor *method, char **errmsg) {
    if (!registry || !method || !method->name || !method->handler) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid method descriptor");
        return 0;
    }
    if (ducknng_method_name_exists(registry, method->name)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: method already registered");
        return 0;
    }
    if (!ducknng_registry_reserve(registry, registry->method_count + 1, errmsg)) return 0;
    registry->methods[registry->method_count++] = method;
    return 1;
}

int ducknng_method_registry_register_many(ducknng_method_registry *registry,
    const ducknng_method_descriptor *const *methods, size_t n_methods, char **errmsg) {
    size_t i;
    if (!registry || (!methods && n_methods > 0)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid bulk method registration request");
        return 0;
    }
    for (i = 0; i < n_methods; i++) {
        if (!ducknng_method_registry_register(registry, methods[i], errmsg)) return 0;
    }
    return 1;
}

const ducknng_method_descriptor *ducknng_method_registry_find(
    const ducknng_method_registry *registry, const uint8_t *name, uint32_t name_len) {
    size_t i;
    if (!registry || !name) return NULL;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *method = registry->methods[i];
        size_t n;
        if (!method || !method->name) continue;
        n = strlen(method->name);
        if ((uint32_t)n == name_len && memcmp(method->name, name, n) == 0) return method;
    }
    return NULL;
}

void ducknng_method_reply_init(ducknng_method_reply *reply) {
    if (!reply) return;
    memset(reply, 0, sizeof(*reply));
    reply->type = DUCKNNG_RPC_RESULT;
    reply->status = DUCKNNG_STATUS_OK;
}

void ducknng_method_reply_reset(ducknng_method_reply *reply) {
    if (!reply) return;
    if (reply->payload) duckdb_free(reply->payload);
    if (reply->error) duckdb_free(reply->error);
    ducknng_method_reply_init(reply);
}

int ducknng_method_reply_set_payload(ducknng_method_reply *reply, uint8_t type, uint32_t flags,
    uint8_t *payload, size_t payload_len) {
    if (!reply) return 0;
    if (reply->payload) duckdb_free(reply->payload);
    reply->type = type;
    reply->flags = flags;
    reply->payload = payload;
    reply->payload_len = payload_len;
    reply->status = DUCKNNG_STATUS_OK;
    return 1;
}

int ducknng_method_reply_set_error(ducknng_method_reply *reply, int32_t status, const char *message) {
    if (!reply) return 0;
    if (reply->error) {
        duckdb_free(reply->error);
        reply->error = NULL;
    }
    reply->type = DUCKNNG_RPC_ERROR;
    reply->flags = 0;
    reply->status = status;
    reply->error = ducknng_strdup(message ? message : "ducknng: unspecified error");
    return reply->error != NULL;
}

const char *ducknng_transport_pattern_name(ducknng_transport_pattern value) {
    switch (value) {
        case DUCKNNG_TRANSPORT_REQREP: return "reqrep";
        case DUCKNNG_TRANSPORT_PUBSUB: return "pubsub";
        default: return "unknown";
    }
}

const char *ducknng_payload_format_name(ducknng_payload_format value) {
    switch (value) {
        case DUCKNNG_PAYLOAD_NONE: return "none";
        case DUCKNNG_PAYLOAD_JSON: return "json";
        case DUCKNNG_PAYLOAD_ARROW_IPC_STREAM: return "arrow_ipc_stream";
        default: return "unknown";
    }
}

const char *ducknng_response_mode_name(ducknng_response_mode value) {
    switch (value) {
        case DUCKNNG_RESPONSE_NONE: return "none";
        case DUCKNNG_RESPONSE_METADATA_ONLY: return "metadata_only";
        case DUCKNNG_RESPONSE_ROWS: return "rows";
        case DUCKNNG_RESPONSE_METADATA_OR_ROWS: return "metadata_or_rows";
        case DUCKNNG_RESPONSE_SESSION_OPEN: return "session_open";
        case DUCKNNG_RESPONSE_EVENT: return "event";
        default: return "unknown";
    }
}

const char *ducknng_session_behavior_name(ducknng_session_behavior value) {
    switch (value) {
        case DUCKNNG_SESSION_STATELESS: return "stateless";
        case DUCKNNG_SESSION_OPENS: return "opens_session";
        case DUCKNNG_SESSION_REQUIRES: return "requires_session";
        case DUCKNNG_SESSION_CLOSES: return "closes_session";
        case DUCKNNG_SESSION_CANCELS: return "cancels_session";
        default: return "unknown";
    }
}

static int append_text(char **buf, size_t *len, size_t *cap, const char *text) {
    size_t add = text ? strlen(text) : 0;
    char *next;
    size_t want;
    if (!text || add == 0) return 1;
    want = *len + add + 1;
    if (*cap >= want) {
        memcpy(*buf + *len, text, add + 1);
        *len += add;
        return 1;
    }
    *cap = *cap ? *cap * 2 : 1024;
    while (*cap < want) *cap *= 2;
    next = (char *)realloc(*buf, *cap);
    if (!next) return 0;
    *buf = next;
    memcpy(*buf + *len, text, add + 1);
    *len += add;
    return 1;
}

static int append_json_string(char **buf, size_t *len, size_t *cap, const char *text) {
    const char *p = text ? text : "";
    if (!append_text(buf, len, cap, "\"")) return 0;
    while (*p) {
        char esc[3] = {0, 0, 0};
        if (*p == '\\' || *p == '"') {
            esc[0] = '\\';
            esc[1] = *p;
            if (!append_text(buf, len, cap, esc)) return 0;
        } else if ((unsigned char)*p < 0x20) {
            char hex[7];
            snprintf(hex, sizeof(hex), "\\u%04x", (unsigned int)(unsigned char)*p);
            if (!append_text(buf, len, cap, hex)) return 0;
        } else {
            char one[2] = {*p, 0};
            if (!append_text(buf, len, cap, one)) return 0;
        }
        p++;
    }
    return append_text(buf, len, cap, "\"");
}

char *ducknng_method_registry_manifest_json(const ducknng_method_registry *registry,
    const char *server_name, const char *server_version, int protocol_version, char **errmsg) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    size_t i;
    char numbuf[64];
    if (!registry) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing method registry");
        return NULL;
    }
    if (!append_text(&buf, &len, &cap, "{\"server\":{\"name\":")) goto oom;
    if (!append_json_string(&buf, &len, &cap, server_name ? server_name : "ducknng")) goto oom;
    if (!append_text(&buf, &len, &cap, ",\"version\":")) goto oom;
    if (!append_json_string(&buf, &len, &cap, server_version ? server_version : "0.1.0")) goto oom;
    if (!append_text(&buf, &len, &cap, ",\"protocol_version\":")) goto oom;
    snprintf(numbuf, sizeof(numbuf), "%d", protocol_version);
    if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
    if (!append_text(&buf, &len, &cap, "},\"methods\":[")) goto oom;
    for (i = 0; i < registry->method_count; i++) {
        const ducknng_method_descriptor *m = registry->methods[i];
        if (!m) continue;
        if (i > 0 && !append_text(&buf, &len, &cap, ",")) goto oom;
        if (!append_text(&buf, &len, &cap, "{")) goto oom;
        if (!append_text(&buf, &len, &cap, "\"name\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, m->name)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"family\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, m->family ? m->family : "")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"summary\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, m->summary ? m->summary : "")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"transport_pattern\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, ducknng_transport_pattern_name(m->transport_pattern))) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"request_payload_format\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, ducknng_payload_format_name(m->request_payload_format))) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"response_payload_format\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, ducknng_payload_format_name(m->response_payload_format))) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"response_mode\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, ducknng_response_mode_name(m->response_mode))) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"session_behavior\":")) goto oom;
        if (!append_json_string(&buf, &len, &cap, ducknng_session_behavior_name(m->session_behavior))) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"requires_auth\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->requires_auth ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"requires_session\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->requires_session ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"opens_session\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->opens_session ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"closes_session\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->closes_session ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"mutates_state\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->mutates_state ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"idempotent\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->idempotent ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"deprecated\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->deprecated ? "true" : "false")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"disabled\":")) goto oom;
        if (!append_text(&buf, &len, &cap, m->disabled ? "true" : "false")) goto oom;
        snprintf(numbuf, sizeof(numbuf), ",\"accepted_request_flags\":%u,\"emitted_reply_flags\":%u,\"max_request_bytes\":%lu,\"max_reply_bytes\":%lu,\"version_introduced\":%d",
            (unsigned int)m->accepted_request_flags,
            (unsigned int)m->emitted_reply_flags,
            (unsigned long)m->max_request_bytes,
            (unsigned long)m->max_reply_bytes,
            m->version_introduced);
        if (!append_text(&buf, &len, &cap, numbuf)) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"request_schema\":")) goto oom;
        if (m->request_schema_json) {
            if (!append_text(&buf, &len, &cap, m->request_schema_json)) goto oom;
        } else if (!append_text(&buf, &len, &cap, "null")) goto oom;
        if (!append_text(&buf, &len, &cap, ",\"response_schema\":")) goto oom;
        if (m->response_schema_json) {
            if (!append_text(&buf, &len, &cap, m->response_schema_json)) goto oom;
        } else if (!append_text(&buf, &len, &cap, "null")) goto oom;
        if (!append_text(&buf, &len, &cap, "}")) goto oom;
    }
    if (!append_text(&buf, &len, &cap, "]}")) goto oom;
    return buf;

oom:
    if (buf) free(buf);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory generating manifest json");
    return NULL;
}
