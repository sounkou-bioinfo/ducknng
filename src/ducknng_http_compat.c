#include "ducknng_http_compat.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <ctype.h>
#include <nng/supplemental/http/http.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

/* Vendored NNG internal helpers kept isolated in the HTTP compat layer. */
extern char *nni_http_res_headers(nng_http_res *res);
extern void nni_strfree(char *s);

static int ducknng_http_tls_requested(const ducknng_tls_opts *opts) {
    return opts && (opts->enabled ||
        (opts->cert_key_file && opts->cert_key_file[0]) ||
        (opts->ca_file && opts->ca_file[0]) ||
        (opts->cert_pem && opts->cert_pem[0]) ||
        (opts->key_pem && opts->key_pem[0]) ||
        (opts->ca_pem && opts->ca_pem[0]) ||
        (opts->password && opts->password[0]) ||
        opts->auth_mode != 0);
}

static int ducknng_http_tls_auth_mode_map(int auth_mode, nng_tls_auth_mode *out) {
    if (!out) return NNG_EINVAL;
    switch (auth_mode) {
    case 0: *out = NNG_TLS_AUTH_MODE_NONE; return 0;
    case 1: *out = NNG_TLS_AUTH_MODE_OPTIONAL; return 0;
    case 2: *out = NNG_TLS_AUTH_MODE_REQUIRED; return 0;
    default: return NNG_EINVAL;
    }
}

static int ducknng_http_tls_config_build(nng_tls_config **out, const char *url, const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    nng_tls_auth_mode auth_mode;
    nng_url *up = NULL;
    int rv;
    if (!out) return NNG_EINVAL;
    *out = NULL;
    if (!opts || !ducknng_http_tls_requested(opts)) return 0;
    rv = nng_tls_config_alloc(&cfg, NNG_TLS_MODE_CLIENT);
    if (rv != 0) return rv;
    rv = ducknng_http_tls_auth_mode_map(opts->auth_mode, &auth_mode);
    if (rv != 0) goto fail;
    rv = nng_tls_config_auth_mode(cfg, auth_mode);
    if (rv != 0) goto fail;
    if (opts->ca_file && opts->ca_file[0]) {
        rv = nng_tls_config_ca_file(cfg, opts->ca_file);
        if (rv != 0) goto fail;
    } else if (opts->ca_pem && opts->ca_pem[0]) {
        rv = nng_tls_config_ca_chain(cfg, opts->ca_pem, NULL);
        if (rv != 0) goto fail;
    }
    if (opts->cert_key_file && opts->cert_key_file[0]) {
        rv = nng_tls_config_cert_key_file(cfg, opts->cert_key_file, opts->password);
        if (rv != 0) goto fail;
    } else if (opts->cert_pem && opts->cert_pem[0] && opts->key_pem && opts->key_pem[0]) {
        rv = nng_tls_config_own_cert(cfg, opts->cert_pem, opts->key_pem, opts->password);
        if (rv != 0) goto fail;
    }
    if (url && nng_url_parse(&up, url) == 0 && up && up->u_hostname && up->u_hostname[0]) {
        rv = nng_tls_config_server_name(cfg, up->u_hostname);
        if (rv != 0) goto fail;
    }
    if (up) nng_url_free(up);
    *out = cfg;
    return 0;
fail:
    if (up) nng_url_free(up);
    if (cfg) nng_tls_config_free(cfg);
    return rv;
}

static void ducknng_http_skip_ws(const char **p) {
    while (p && *p && **p && isspace((unsigned char)**p)) (*p)++;
}

static int ducknng_http_buf_append(char **buf, size_t *len, size_t *cap, const char *src, size_t src_len) {
    char *new_buf;
    size_t new_cap;
    if (!buf || !len || !cap) return -1;
    if (*cap < *len + src_len + 1) {
        new_cap = *cap ? *cap * 2 : 128;
        while (new_cap < *len + src_len + 1) new_cap *= 2;
        new_buf = (char *)duckdb_malloc(new_cap);
        if (!new_buf) return -1;
        if (*buf && *len) memcpy(new_buf, *buf, *len);
        if (*buf) duckdb_free(*buf);
        *buf = new_buf;
        *cap = new_cap;
    }
    if (src_len) memcpy(*buf + *len, src, src_len);
    *len += src_len;
    (*buf)[*len] = '\0';
    return 0;
}

static int ducknng_http_append_json_string(char **buf, size_t *len, size_t *cap, const char *src) {
    size_t i;
    if (ducknng_http_buf_append(buf, len, cap, "\"", 1) != 0) return -1;
    if (!src) src = "";
    for (i = 0; src[i]; i++) {
        unsigned char c = (unsigned char)src[i];
        switch (c) {
        case '"': if (ducknng_http_buf_append(buf, len, cap, "\\\"", 2) != 0) return -1; break;
        case '\\': if (ducknng_http_buf_append(buf, len, cap, "\\\\", 2) != 0) return -1; break;
        case '\b': if (ducknng_http_buf_append(buf, len, cap, "\\b", 2) != 0) return -1; break;
        case '\f': if (ducknng_http_buf_append(buf, len, cap, "\\f", 2) != 0) return -1; break;
        case '\n': if (ducknng_http_buf_append(buf, len, cap, "\\n", 2) != 0) return -1; break;
        case '\r': if (ducknng_http_buf_append(buf, len, cap, "\\r", 2) != 0) return -1; break;
        case '\t': if (ducknng_http_buf_append(buf, len, cap, "\\t", 2) != 0) return -1; break;
        default:
            if (c < 0x20) {
                char esc[7];
                snprintf(esc, sizeof(esc), "\\u%04x", (unsigned)c);
                if (ducknng_http_buf_append(buf, len, cap, esc, 6) != 0) return -1;
            } else {
                char one = (char)c;
                if (ducknng_http_buf_append(buf, len, cap, &one, 1) != 0) return -1;
            }
        }
    }
    if (ducknng_http_buf_append(buf, len, cap, "\"", 1) != 0) return -1;
    return 0;
}

static char *ducknng_http_parse_json_string(const char **p, char **errmsg) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    if (!p || !*p || **p != '"') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected JSON string");
        return NULL;
    }
    (*p)++;
    while (**p) {
        char c = *(*p)++;
        if (c == '"') return buf ? buf : ducknng_strdup("");
        if (c == '\\') {
            char esc = *(*p)++;
            char out;
            switch (esc) {
            case '"': out = '"'; break;
            case '\\': out = '\\'; break;
            case '/': out = '/'; break;
            case 'b': out = '\b'; break;
            case 'f': out = '\f'; break;
            case 'n': out = '\n'; break;
            case 'r': out = '\r'; break;
            case 't': out = '\t'; break;
            case 'u': {
                int i;
                unsigned value = 0;
                for (i = 0; i < 4; i++) {
                    char h = *(*p)++;
                    if (h >= '0' && h <= '9') value = (value << 4) | (unsigned)(h - '0');
                    else if (h >= 'a' && h <= 'f') value = (value << 4) | (unsigned)(h - 'a' + 10);
                    else if (h >= 'A' && h <= 'F') value = (value << 4) | (unsigned)(h - 'A' + 10);
                    else {
                        if (buf) duckdb_free(buf);
                        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid JSON unicode escape in headers_json");
                        return NULL;
                    }
                }
                if (value > 0x7f) {
                    if (buf) duckdb_free(buf);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: only ASCII JSON unicode escapes are supported in headers_json");
                    return NULL;
                }
                out = (char)value;
                break;
            }
            default:
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported JSON escape in headers_json");
                return NULL;
            }
            if (ducknng_http_buf_append(&buf, &len, &cap, &out, 1) != 0) {
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing headers_json");
                return NULL;
            }
        } else {
            if (ducknng_http_buf_append(&buf, &len, &cap, &c, 1) != 0) {
                if (buf) duckdb_free(buf);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing headers_json");
                return NULL;
            }
        }
    }
    if (buf) duckdb_free(buf);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: unterminated JSON string in headers_json");
    return NULL;
}

static int ducknng_http_apply_headers_json(nng_http_req *req, const char *headers_json, char **errmsg) {
    const char *p = headers_json;
    if (errmsg) *errmsg = NULL;
    if (!headers_json || !headers_json[0]) return 0;
    ducknng_http_skip_ws(&p);
    if (*p != '[') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: headers_json must be a JSON array of {name,value} objects");
        return -1;
    }
    p++;
    ducknng_http_skip_ws(&p);
    if (*p == ']') return 0;
    for (;;) {
        char *key = NULL;
        char *value = NULL;
        char *name = NULL;
        char *header_value = NULL;
        int rv;
        ducknng_http_skip_ws(&p);
        if (*p != '{') {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: expected header object in headers_json");
            return -1;
        }
        p++;
        for (;;) {
            ducknng_http_skip_ws(&p);
            key = ducknng_http_parse_json_string(&p, errmsg);
            if (!key) return -1;
            ducknng_http_skip_ws(&p);
            if (*p != ':') {
                duckdb_free(key);
                if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: expected ':' in headers_json");
                return -1;
            }
            p++;
            ducknng_http_skip_ws(&p);
            value = ducknng_http_parse_json_string(&p, errmsg);
            if (!value) {
                duckdb_free(key);
                return -1;
            }
            if (strcmp(key, "name") == 0) {
                if (name) {
                    duckdb_free(key);
                    duckdb_free(value);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: duplicate header name field in headers_json");
                    if (header_value) duckdb_free(header_value);
                    return -1;
                }
                name = value;
            } else if (strcmp(key, "value") == 0) {
                if (header_value) {
                    duckdb_free(key);
                    duckdb_free(value);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: duplicate header value field in headers_json");
                    if (name) duckdb_free(name);
                    return -1;
                }
                header_value = value;
            } else {
                duckdb_free(value);
                duckdb_free(key);
                if (name) duckdb_free(name);
                if (header_value) duckdb_free(header_value);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: headers_json objects may contain only name and value fields");
                return -1;
            }
            duckdb_free(key);
            ducknng_http_skip_ws(&p);
            if (*p == ',') {
                p++;
                continue;
            }
            if (*p == '}') {
                p++;
                break;
            }
            if (name) duckdb_free(name);
            if (header_value) duckdb_free(header_value);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or '}' in headers_json");
            return -1;
        }
        if (!name || !name[0] || !header_value) {
            if (name) duckdb_free(name);
            if (header_value) duckdb_free(header_value);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: each headers_json object must contain non-empty name and value strings");
            return -1;
        }
        rv = nng_http_req_add_header(req, name, header_value);
        duckdb_free(name);
        duckdb_free(header_value);
        if (rv != 0) {
            if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
            return -1;
        }
        ducknng_http_skip_ws(&p);
        if (*p == ',') {
            p++;
            continue;
        }
        if (*p == ']') return 0;
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or ']' in headers_json");
        return -1;
    }
}

static char *ducknng_http_headers_block_to_json(const char *headers_block, char **errmsg) {
    char *buf = NULL;
    size_t len = 0;
    size_t cap = 0;
    const char *line = headers_block;
    int first = 1;
    if (errmsg) *errmsg = NULL;
    if (ducknng_http_buf_append(&buf, &len, &cap, "[", 1) != 0) goto oom;
    if (!headers_block || !headers_block[0]) {
        if (ducknng_http_buf_append(&buf, &len, &cap, "]", 1) != 0) goto oom;
        return buf;
    }
    while (*line) {
        const char *line_end = strstr(line, "\r\n");
        const char *colon;
        const char *value;
        size_t name_len;
        size_t value_len;
        char *name_copy;
        char *value_copy;
        if (!line_end) line_end = line + strlen(line);
        if (line_end == line) break;
        colon = memchr(line, ':', (size_t)(line_end - line));
        if (!colon) {
            if (line_end[0] == '\0') break;
            line = line_end[0] ? line_end + 2 : line_end;
            continue;
        }
        name_len = (size_t)(colon - line);
        value = colon + 1;
        while (value < line_end && (*value == ' ' || *value == '\t')) value++;
        value_len = (size_t)(line_end - value);
        name_copy = (char *)duckdb_malloc(name_len + 1);
        value_copy = (char *)duckdb_malloc(value_len + 1);
        if (!name_copy || !value_copy) {
            if (name_copy) duckdb_free(name_copy);
            if (value_copy) duckdb_free(value_copy);
            goto oom;
        }
        memcpy(name_copy, line, name_len);
        name_copy[name_len] = '\0';
        memcpy(value_copy, value, value_len);
        value_copy[value_len] = '\0';
        if (!first && ducknng_http_buf_append(&buf, &len, &cap, ",", 1) != 0) {
            duckdb_free(name_copy);
            duckdb_free(value_copy);
            goto oom;
        }
        first = 0;
        if (ducknng_http_buf_append(&buf, &len, &cap, "{\"name\":", 8) != 0 ||
            ducknng_http_append_json_string(&buf, &len, &cap, name_copy) != 0 ||
            ducknng_http_buf_append(&buf, &len, &cap, ",\"value\":", 9) != 0 ||
            ducknng_http_append_json_string(&buf, &len, &cap, value_copy) != 0 ||
            ducknng_http_buf_append(&buf, &len, &cap, "}", 1) != 0) {
            duckdb_free(name_copy);
            duckdb_free(value_copy);
            goto oom;
        }
        duckdb_free(name_copy);
        duckdb_free(value_copy);
        line = line_end[0] ? line_end + 2 : line_end;
    }
    if (ducknng_http_buf_append(&buf, &len, &cap, "]", 1) != 0) goto oom;
    return buf;
oom:
    if (buf) duckdb_free(buf);
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory building headers_json");
    return NULL;
}

int ducknng_validate_http_url(const char *url, char **errmsg) {
    ducknng_transport_url parsed;
    char *parse_err = NULL;
    if (errmsg) *errmsg = NULL;
    if (ducknng_transport_url_parse(url, &parsed, &parse_err) != 0) {
        if (errmsg) *errmsg = parse_err;
        else if (parse_err) duckdb_free(parse_err);
        return -1;
    }
    if (!ducknng_transport_url_is_http(&parsed)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: http:// or https:// URL is required");
        return -1;
    }
    return 0;
}

int ducknng_http_transact(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    uint16_t *out_status, char **out_headers_json, uint8_t **out_body, size_t *out_body_len,
    char **errmsg) {
    ducknng_transport_url parsed;
    nng_url *parsed_url = NULL;
    nng_http_client *client = NULL;
    nng_http_req *req = NULL;
    nng_http_res *res = NULL;
    nng_aio *aio = NULL;
    nng_tls_config *tls_cfg = NULL;
    char *header_block = NULL;
    void *resp_body = NULL;
    size_t resp_body_len = 0;
    int rv;
    if (out_status) *out_status = 0;
    if (out_headers_json) *out_headers_json = NULL;
    if (out_body) *out_body = NULL;
    if (out_body_len) *out_body_len = 0;
    if (errmsg) *errmsg = NULL;
    if (ducknng_validate_http_url(url, errmsg) != 0) return -1;
    if (ducknng_transport_url_parse(url, &parsed, errmsg) != 0) return -1;
    if (ducknng_http_tls_requested(tls_opts) && !parsed.uses_tls) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires an https:// URL");
        return -1;
    }
    rv = nng_url_parse(&parsed_url, url);
    if (rv != 0) {
        if (errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    rv = nng_http_client_alloc(&client, parsed_url);
    if (rv != 0) goto fail;
    rv = nng_http_req_alloc(&req, parsed_url);
    if (rv != 0) goto fail;
    rv = nng_http_res_alloc(&res);
    if (rv != 0) goto fail;
    if (method && method[0]) {
        rv = nng_http_req_set_method(req, method);
        if (rv != 0) goto fail;
    }
    if (headers_json && headers_json[0]) {
        if (ducknng_http_apply_headers_json(req, headers_json, errmsg) != 0) {
            rv = NNG_EINVAL;
            goto fail;
        }
    }
    if (body && body_len > 0) {
        rv = nng_http_req_copy_data(req, body, body_len);
        if (rv != 0) goto fail;
    }
    if (parsed.uses_tls && ducknng_http_tls_requested(tls_opts)) {
        rv = ducknng_http_tls_config_build(&tls_cfg, url, tls_opts);
        if (rv != 0) goto fail;
        if (tls_cfg) {
            rv = nng_http_client_set_tls(client, tls_cfg);
            nng_tls_config_free(tls_cfg);
            tls_cfg = NULL;
            if (rv != 0) goto fail;
        }
    }
    rv = ducknng_aio_alloc(&aio, NULL, NULL, timeout_ms);
    if (rv != 0) goto fail;
    nng_http_client_transact(client, req, res, aio);
    ducknng_aio_wait(aio);
    rv = ducknng_aio_result(aio);
    if (rv != 0) goto fail;
    if (out_status) *out_status = nng_http_res_get_status(res);
    header_block = nni_http_res_headers(res);
    if (out_headers_json) {
        *out_headers_json = ducknng_http_headers_block_to_json(header_block, errmsg);
        if (!*out_headers_json && header_block) {
            rv = NNG_ENOMEM;
            goto fail;
        }
    }
    nng_http_res_get_data(res, &resp_body, &resp_body_len);
    if (out_body_len) *out_body_len = resp_body_len;
    if (out_body) {
        *out_body = (uint8_t *)duckdb_malloc(resp_body_len);
        if (!*out_body && resp_body_len > 0) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying HTTP response body");
            rv = NNG_ENOMEM;
            goto fail;
        }
        if (resp_body_len) memcpy(*out_body, resp_body, resp_body_len);
    }
    if (header_block) nni_strfree(header_block);
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    if (client) nng_http_client_free(client);
    if (parsed_url) nng_url_free(parsed_url);
    return 0;
fail:
    if (tls_cfg) nng_tls_config_free(tls_cfg);
    if (header_block) nni_strfree(header_block);
    if (out_headers_json && *out_headers_json) {
        duckdb_free(*out_headers_json);
        *out_headers_json = NULL;
    }
    if (out_body && *out_body) {
        duckdb_free(*out_body);
        *out_body = NULL;
    }
    if (out_body_len) *out_body_len = 0;
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    if (aio) ducknng_aio_free(aio);
    if (res) nng_http_res_free(res);
    if (req) nng_http_req_free(req);
    if (client) nng_http_client_free(client);
    if (parsed_url) nng_url_free(parsed_url);
    return -1;
}
