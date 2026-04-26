#include "ducknng_service.h"
#include "ducknng_registry.h"
#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include <ws2tcpip.h>
#else
#include <arpa/inet.h>
#endif

DUCKDB_EXTENSION_EXTERN

static void ducknng_service_clear_ip_allowlist(ducknng_service *svc) {
    if (!svc) return;
    if (svc->ip_allowlist) duckdb_free(svc->ip_allowlist);
    if (svc->ip_allowlist_json) duckdb_free(svc->ip_allowlist_json);
    svc->ip_allowlist = NULL;
    svc->ip_allowlist_count = 0;
    svc->ip_allowlist_json = NULL;
    svc->ip_allowlist_active = 0;
}

static void ducknng_json_skip_ws(const char **p) {
    while (p && *p && (**p == ' ' || **p == '\t' || **p == '\r' || **p == '\n')) (*p)++;
}

static char *ducknng_json_parse_ascii_string(const char **p, char **errmsg) {
    const char *s;
    char *out;
    size_t len = 0;
    size_t cap = 32;
    if (!p || !*p || **p != '"') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist must be a JSON array of strings");
        return NULL;
    }
    s = ++(*p);
    out = (char *)duckdb_malloc(cap);
    if (!out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing IP allowlist");
        return NULL;
    }
    while (*s) {
        unsigned char ch = (unsigned char)*s++;
        if (ch == '"') {
            out[len] = '\0';
            *p = s;
            return out;
        }
        if (ch == '\\') {
            char esc = *s++;
            if (!esc) break;
            switch (esc) {
                case '"': ch = '"'; break;
                case '\\': ch = '\\'; break;
                case '/': ch = '/'; break;
                case 'b': ch = '\b'; break;
                case 'f': ch = '\f'; break;
                case 'n': ch = '\n'; break;
                case 'r': ch = '\r'; break;
                case 't': ch = '\t'; break;
                default:
                    duckdb_free(out);
                    if (errmsg) *errmsg = ducknng_strdup("ducknng: unsupported JSON escape in IP allowlist");
                    return NULL;
            }
        } else if (ch < 0x20) {
            duckdb_free(out);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid control character in IP allowlist");
            return NULL;
        }
        if (len + 2 > cap) {
            char *next;
            cap *= 2;
            next = (char *)duckdb_malloc(cap);
            if (!next) {
                duckdb_free(out);
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing IP allowlist");
                return NULL;
            }
            memcpy(next, out, len);
            duckdb_free(out);
            out = next;
        }
        out[len++] = (char)ch;
    }
    duckdb_free(out);
    if (errmsg) *errmsg = ducknng_strdup("ducknng: unterminated JSON string in IP allowlist");
    return NULL;
}

static int ducknng_parse_u8_decimal(const char *s, int max_value, int *out) {
    int v = 0;
    if (!s || !s[0]) return -1;
    while (*s) {
        if (*s < '0' || *s > '9') return -1;
        v = v * 10 + (*s - '0');
        if (v > max_value) return -1;
        s++;
    }
    if (out) *out = v;
    return 0;
}

static int ducknng_parse_ip_rule(const char *text, ducknng_ip_allow_rule *rule, char **errmsg) {
    char host[128];
    const char *slash;
    size_t host_len;
    int prefix = -1;
    if (!text || !text[0] || !rule) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist entries must be non-empty strings");
        return -1;
    }
    slash = strchr(text, '/');
    host_len = slash ? (size_t)(slash - text) : strlen(text);
    if (host_len == 0 || host_len >= sizeof(host)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid IP allowlist address");
        return -1;
    }
    memcpy(host, text, host_len);
    host[host_len] = '\0';
    memset(rule, 0, sizeof(*rule));
    if (inet_pton(AF_INET, host, rule->addr) == 1) {
        rule->family = NNG_AF_INET;
        prefix = slash ? -1 : 32;
        if (slash && ducknng_parse_u8_decimal(slash + 1, 32, &prefix) != 0) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid IPv4 CIDR prefix in IP allowlist");
            return -1;
        }
    } else if (inet_pton(AF_INET6, host, rule->addr) == 1) {
        rule->family = NNG_AF_INET6;
        prefix = slash ? -1 : 128;
        if (slash && ducknng_parse_u8_decimal(slash + 1, 128, &prefix) != 0) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid IPv6 CIDR prefix in IP allowlist");
            return -1;
        }
    } else {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist entries must be IPv4/IPv6 literals or CIDR blocks");
        return -1;
    }
    rule->prefix_bits = (uint8_t)prefix;
    return 0;
}

static void ducknng_ip_rules_free(ducknng_ip_allow_rule *rules) {
    if (rules) duckdb_free(rules);
}

static int ducknng_parse_ip_allowlist_json(const char *json, ducknng_ip_allow_rule **out_rules,
    size_t *out_count, char **out_json, char **errmsg) {
    const char *p = json;
    ducknng_ip_allow_rule *rules = NULL;
    size_t count = 0;
    size_t cap = 0;
    if (out_rules) *out_rules = NULL;
    if (out_count) *out_count = 0;
    if (out_json) *out_json = NULL;
    if (errmsg) *errmsg = NULL;
    ducknng_json_skip_ws(&p);
    if (!p || *p != '[') {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: IP allowlist must be a JSON array of strings");
        return -1;
    }
    p++;
    ducknng_json_skip_ws(&p);
    if (*p == ']') {
        p++;
        ducknng_json_skip_ws(&p);
        if (*p) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after IP allowlist JSON");
            return -1;
        }
        if (out_json) {
            *out_json = ducknng_strdup(json);
            if (!*out_json) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying IP allowlist");
                return -1;
            }
        }
        return 0;
    }
    for (;;) {
        char *item;
        ducknng_ip_allow_rule rule;
        ducknng_json_skip_ws(&p);
        item = ducknng_json_parse_ascii_string(&p, errmsg);
        if (!item) goto fail;
        if (ducknng_parse_ip_rule(item, &rule, errmsg) != 0) {
            duckdb_free(item);
            goto fail;
        }
        duckdb_free(item);
        if (count == cap) {
            ducknng_ip_allow_rule *next;
            size_t new_cap = cap ? cap * 2 : 4;
            next = (ducknng_ip_allow_rule *)duckdb_malloc(sizeof(*next) * new_cap);
            if (!next) {
                if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory parsing IP allowlist");
                goto fail;
            }
            if (rules && count) memcpy(next, rules, sizeof(*rules) * count);
            if (rules) duckdb_free(rules);
            rules = next;
            cap = new_cap;
        }
        rules[count++] = rule;
        ducknng_json_skip_ws(&p);
        if (*p == ',') { p++; continue; }
        if (*p == ']') { p++; break; }
        if (errmsg) *errmsg = ducknng_strdup("ducknng: expected ',' or ']' in IP allowlist JSON");
        goto fail;
    }
    ducknng_json_skip_ws(&p);
    if (*p) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: trailing characters after IP allowlist JSON");
        goto fail;
    }
    if (out_json) {
        *out_json = ducknng_strdup(json);
        if (!*out_json) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory copying IP allowlist");
            goto fail;
        }
    }
    if (out_rules) *out_rules = rules;
    else ducknng_ip_rules_free(rules);
    if (out_count) *out_count = count;
    return 0;
fail:
    ducknng_ip_rules_free(rules);
    return -1;
}

static int ducknng_ip_rule_matches_bytes(const ducknng_ip_allow_rule *rule, const uint8_t *addr) {
    int full;
    int rem;
    uint8_t mask;
    if (!rule || !addr) return 0;
    full = rule->prefix_bits / 8;
    rem = rule->prefix_bits % 8;
    if (full > 0 && memcmp(rule->addr, addr, (size_t)full) != 0) return 0;
    if (rem == 0) return 1;
    mask = (uint8_t)(0xffu << (8 - rem));
    return (rule->addr[full] & mask) == (addr[full] & mask);
}

static int ducknng_remote_addr_matches_ip_allowlist(const ducknng_service *svc, const nng_sockaddr *addr) {
    uint8_t ip[16];
    size_t i;
    if (!svc || !svc->ip_allowlist_active) return 1;
    if (!addr) return 0;
    memset(ip, 0, sizeof(ip));
    if (addr->s_family == NNG_AF_INET) {
        memcpy(ip, &addr->s_in.sa_addr, 4);
    } else if (addr->s_family == NNG_AF_INET6) {
        memcpy(ip, addr->s_in6.sa_addr, 16);
    } else {
        return 0;
    }
    for (i = 0; i < svc->ip_allowlist_count; i++) {
        const ducknng_ip_allow_rule *rule = &svc->ip_allowlist[i];
        if (rule->family == addr->s_family && ducknng_ip_rule_matches_bytes(rule, ip)) return 1;
    }
    return 0;
}

static int ducknng_pipe_remote_addr(nng_pipe pipe, nng_sockaddr *out) {
    if (!out || pipe.id <= 0) return -1;
    memset(out, 0, sizeof(*out));
    return nng_pipe_get_addr(pipe, NNG_OPT_REMADDR, out) == 0 ? 0 : -1;
}

static nng_msg *ducknng_dispatch_request(ducknng_service *svc, const ducknng_frame *frame,
    const char *caller_identity) {
    const ducknng_method_descriptor *method = NULL;
    ducknng_method_descriptor method_snapshot;
    ducknng_request_context req_ctx;
    ducknng_method_reply reply;
    nng_msg *msg;

    memset(&req_ctx, 0, sizeof(req_ctx));
    ducknng_method_reply_init(&reply);

    if (!svc || !svc->rt || !frame) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing dispatcher state");
    }
    {
        char *admission_err = NULL;
        if (ducknng_service_peer_admission_check(svc, caller_identity, &admission_err) != 0) {
            nng_msg *err = ducknng_error_msg(NULL, DUCKNNG_STATUS_UNAUTHORIZED,
                admission_err ? admission_err : "ducknng: peer identity is not admitted");
            if (admission_err) duckdb_free(admission_err);
            return err;
        }
    }
    ducknng_service_prune_idle_sessions(svc, ducknng_now_ms());
    memset(&method_snapshot, 0, sizeof(method_snapshot));
    ducknng_mutex_lock(&svc->rt->mu);
    if (frame->type == DUCKNNG_RPC_MANIFEST) {
        method = ducknng_method_registry_find(&svc->rt->registry,
            (const uint8_t *)"manifest", (uint32_t)strlen("manifest"));
    } else if (frame->type == DUCKNNG_RPC_CALL) {
        method = ducknng_method_registry_find(&svc->rt->registry, frame->name, frame->name_len);
    } else {
        ducknng_mutex_unlock(&svc->rt->mu);
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INVALID,
            "ducknng: unsupported message type");
    }
    if (method) {
        method_snapshot = *method;
        method = &method_snapshot;
    }
    ducknng_mutex_unlock(&svc->rt->mu);
    if (!method) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_NOT_FOUND,
            "ducknng: unknown RPC method");
    }
    if (method->disabled) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_DISABLED,
            "ducknng: RPC method is disabled");
    }
    if (frame->flags & ~method->accepted_request_flags) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INVALID,
            "ducknng: request used unsupported flag bits for this method");
    }
    if (method->max_request_bytes > 0 && frame->payload_len > (uint64_t)method->max_request_bytes) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INVALID,
            "ducknng: request payload exceeds method limit");
    }
    if (method->requires_auth && (!caller_identity || !caller_identity[0])) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_UNAUTHORIZED,
            "ducknng: method requires verified peer identity");
    }

    req_ctx.frame = frame;
    req_ctx.caller_identity = caller_identity;
    if (method->handler(svc, method, &req_ctx, &reply) != 0 && reply.type != DUCKNNG_RPC_ERROR) {
        ducknng_method_reply_reset(&reply);
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
            "ducknng: method handler failed without structured error reply");
    }
    if (reply.type == DUCKNNG_RPC_RESULT && (reply.flags & ~method->emitted_reply_flags)) {
        ducknng_method_reply_reset(&reply);
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
            "ducknng: method emitted unsupported reply flags");
    }
    if (method->max_reply_bytes > 0 && reply.payload_len > method->max_reply_bytes) {
        ducknng_method_reply_reset(&reply);
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_BUSY,
            "ducknng: reply payload exceeds method limit");
    }

    msg = ducknng_build_reply(reply.type, method->name, reply.flags, reply.error,
        reply.payload, (uint64_t)reply.payload_len);
    ducknng_method_reply_reset(&reply);
    if (!msg) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_INTERNAL,
            "ducknng: failed to allocate reply message");
    }
    return msg;
}

static void ducknng_rep_aio_cb(void *arg) {
    ducknng_rep_ctx *rc = (ducknng_rep_ctx *)arg;
    int rv = nng_aio_result(rc->aio);
    ducknng_mutex_lock(&rc->mu);
    if (rc->stopping) {
        ducknng_mutex_unlock(&rc->mu);
        return;
    }
    if (rv != 0) {
        nng_msg *pending_msg = nng_aio_get_msg(rc->aio);
        if (pending_msg) {
            nng_msg_free(pending_msg);
            nng_aio_set_msg(rc->aio, NULL);
        }
        rc->last_nng_err = rv;
        rc->event = DUCKNNG_EVT_NNG_ERROR;
        ducknng_cond_signal(&rc->cv);
        ducknng_mutex_unlock(&rc->mu);
        return;
    }
    if (rc->phase == DUCKNNG_PHASE_RECV) {
        rc->request_msg = nng_aio_get_msg(rc->aio);
        nng_aio_set_msg(rc->aio, NULL);
        rc->event = DUCKNNG_EVT_REQUEST_READY;
        ducknng_cond_signal(&rc->cv);
        ducknng_mutex_unlock(&rc->mu);
        return;
    }
    rc->event = DUCKNNG_EVT_SEND_DONE;
    ducknng_cond_signal(&rc->cv);
    ducknng_mutex_unlock(&rc->mu);
}

static void ducknng_rep_ctx_teardown(ducknng_rep_ctx *rc) {
    if (!rc) return;
    if (rc->mu_initialized) {
        ducknng_mutex_lock(&rc->mu);
        rc->stopping = 1;
        ducknng_cond_signal(&rc->cv);
        ducknng_mutex_unlock(&rc->mu);
    }
    if (rc->aio) ducknng_aio_cancel(rc->aio);
    if (rc->thread_started) {
        ducknng_thread_join(rc->thread);
        rc->thread_started = 0;
    }
    if (rc->aio) {
        ducknng_aio_wait(rc->aio);
        if (nng_aio_get_msg(rc->aio)) {
            nng_msg_free(nng_aio_get_msg(rc->aio));
            nng_aio_set_msg(rc->aio, NULL);
        }
    }
    if (rc->request_msg) {
        nng_msg_free(rc->request_msg);
        rc->request_msg = NULL;
    }
    if (rc->aio) {
        ducknng_aio_free(rc->aio);
        rc->aio = NULL;
    }
    if (rc->ctx.id != 0) {
        ducknng_ctx_close(rc->ctx);
        memset(&rc->ctx, 0, sizeof(rc->ctx));
    }
    if (rc->cv_initialized) {
        ducknng_cond_destroy(&rc->cv);
        rc->cv_initialized = 0;
    }
    if (rc->mu_initialized) {
        ducknng_mutex_destroy(&rc->mu);
        rc->mu_initialized = 0;
    }
}

nng_msg *ducknng_handle_request_with_identity(ducknng_service *svc, nng_msg *req,
    const char *caller_identity) {
    ducknng_frame frame;
    if (ducknng_decode_request(req, &frame) != 0) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INVALID, "invalid RPC envelope");
    }
    return ducknng_dispatch_request(svc, &frame, caller_identity);
}

nng_msg *ducknng_handle_request(ducknng_service *svc, nng_msg *req) {
    nng_pipe pipe;
    nng_sockaddr remote_addr;
    memset(&pipe, 0, sizeof(pipe));
    if (req) pipe = nng_msg_get_pipe(req);
    int have_remote_addr = ducknng_pipe_remote_addr(pipe, &remote_addr) == 0;
    char *caller_identity = ducknng_msg_verified_peer_identity(req);
    char *admission_err = NULL;
    nng_msg *reply;
    if (ducknng_service_network_admission_check(svc, caller_identity,
            have_remote_addr ? &remote_addr : NULL, &admission_err) != 0) {
        reply = ducknng_error_msg(NULL, DUCKNNG_STATUS_UNAUTHORIZED,
            admission_err ? admission_err : "ducknng: peer is not admitted");
        if (admission_err) duckdb_free(admission_err);
        if (caller_identity) duckdb_free(caller_identity);
        return reply;
    }
    reply = ducknng_handle_request_with_identity(svc, req, caller_identity);
    if (caller_identity) duckdb_free(caller_identity);
    return reply;
}

static void *ducknng_rep_worker_main(void *arg) {
    ducknng_rep_ctx *rc = (ducknng_rep_ctx *)arg;
    ducknng_mutex_lock(&rc->mu);
    rc->phase = DUCKNNG_PHASE_RECV;
    ducknng_mutex_unlock(&rc->mu);
    nng_ctx_recv(rc->ctx, rc->aio);
    for (;;) {
        int event, nng_err, stopping;
        nng_msg *req = NULL;
        ducknng_mutex_lock(&rc->mu);
        while (rc->event == DUCKNNG_EVT_NONE && !rc->stopping) {
            ducknng_cond_wait(&rc->cv, &rc->mu);
        }
        event = rc->event;
        rc->event = DUCKNNG_EVT_NONE;
        nng_err = rc->last_nng_err;
        rc->last_nng_err = 0;
        req = rc->request_msg;
        rc->request_msg = NULL;
        stopping = rc->stopping;
        ducknng_mutex_unlock(&rc->mu);
        if (stopping) break;
        if (event == DUCKNNG_EVT_NNG_ERROR) {
            if (nng_err == NNG_ECLOSED) break;
            ducknng_mutex_lock(&rc->mu);
            rc->phase = DUCKNNG_PHASE_RECV;
            ducknng_mutex_unlock(&rc->mu);
            nng_ctx_recv(rc->ctx, rc->aio);
            continue;
        }
        if (event == DUCKNNG_EVT_REQUEST_READY) {
            nng_msg *resp = ducknng_handle_request(rc->svc, req);
            nng_msg_free(req);
            ducknng_mutex_lock(&rc->mu);
            rc->phase = DUCKNNG_PHASE_SEND;
            ducknng_mutex_unlock(&rc->mu);
            nng_aio_set_msg(rc->aio, resp);
            nng_ctx_send(rc->ctx, rc->aio);
            continue;
        }
        if (event == DUCKNNG_EVT_SEND_DONE) {
            ducknng_mutex_lock(&rc->mu);
            rc->phase = DUCKNNG_PHASE_RECV;
            ducknng_mutex_unlock(&rc->mu);
            nng_ctx_recv(rc->ctx, rc->aio);
            continue;
        }
    }
    return NULL;
}

ducknng_service *ducknng_service_create(ducknng_runtime *rt, const char *name, const char *listen_url,
    int contexts, size_t recv_max_bytes, uint64_t session_idle_ms,
    uint64_t tls_config_id, const char *tls_config_source, const ducknng_tls_opts *tls_opts) {
    ducknng_service *svc = (ducknng_service *)duckdb_malloc(sizeof(*svc));
    if (!svc) return NULL;
    memset(svc, 0, sizeof(*svc));
    atomic_store_explicit(&svc->session_count_visible, 0, memory_order_release);
    ducknng_tls_opts_init(&svc->tls_opts);
    svc->rt = rt;
    svc->name = ducknng_strdup(name);
    svc->listen_url = ducknng_strdup(listen_url);
    svc->ncontexts = contexts > 0 ? contexts : 1;
    svc->recv_max_bytes = recv_max_bytes ? recv_max_bytes : 134217728u;
    svc->session_idle_ms = session_idle_ms ? session_idle_ms : 300000u;
    svc->tls_config_id = tls_config_id;
    svc->next_session_id = 1;
    svc->tls_config_source = ducknng_strdup(tls_config_source);
    if ((tls_config_source && !svc->tls_config_source) || ducknng_tls_opts_copy(&svc->tls_opts, tls_opts) != 0) {
        ducknng_service_destroy(svc);
        return NULL;
    }
    if (ducknng_mutex_init(&svc->mu) != 0) {
        ducknng_service_destroy(svc);
        return NULL;
    }
    svc->mu_initialized = 1;
    return svc;
}

void ducknng_service_destroy(ducknng_service *svc) {
    ducknng_session **sessions = NULL;
    size_t session_count = 0;
    size_t i;
    if (!svc) return;
    if (svc->running || svc->http_state || svc->ctxs || svc->listener.id != 0 || svc->rep_sock.id != 0) {
        (void)ducknng_service_stop(svc, NULL);
    }
    sessions = ducknng_service_detach_all_sessions(svc, &session_count);
    if (svc->name) duckdb_free(svc->name);
    if (svc->listen_url) duckdb_free(svc->listen_url);
    if (svc->resolved_listen_url) duckdb_free(svc->resolved_listen_url);
    if (svc->tls_config_source) duckdb_free(svc->tls_config_source);
    ducknng_tls_opts_reset(&svc->tls_opts);
    ducknng_service_clear_ip_allowlist(svc);
    for (i = 0; i < session_count; i++) {
        if (sessions && sessions[i]) ducknng_session_destroy(sessions[i]);
    }
    if (sessions) duckdb_free(sessions);
    if (svc->ctxs) duckdb_free(svc->ctxs);
    if (svc->mu_initialized) {
        ducknng_mutex_destroy(&svc->mu);
        svc->mu_initialized = 0;
    }
    duckdb_free(svc);
}

static void ducknng_service_pipe_add_pre_cb(nng_pipe pipe, nng_pipe_ev ev, void *arg) {
    ducknng_service *svc = (ducknng_service *)arg;
    char *identity = NULL;
    nng_sockaddr remote_addr;
    int have_remote_addr = 0;
    int allowed = 1;
    if (ev != NNG_PIPE_EV_ADD_PRE || !svc) return;
    identity = ducknng_pipe_verified_peer_identity(pipe);
    have_remote_addr = ducknng_pipe_remote_addr(pipe, &remote_addr) == 0;
    allowed = ducknng_service_network_admission_check(svc, identity,
        have_remote_addr ? &remote_addr : NULL, NULL) == 0;
    if (identity) duckdb_free(identity);
    if (!allowed) (void)nng_pipe_close(pipe);
}

int ducknng_service_start(ducknng_service *svc, char **errmsg) {
    int rv;
    int i;
    ducknng_tls_opts tls_opts;
    ducknng_transport_url transport;
    char *parse_err = NULL;
    if (!svc) return -1;

    ducknng_tls_opts_init(&tls_opts);
    if (ducknng_tls_opts_copy(&tls_opts, &svc->tls_opts) != 0) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: failed to copy TLS configuration");
        return -1;
    }
    tls_opts.enabled = tls_opts.enabled ||
        (tls_opts.cert_key_file && tls_opts.cert_key_file[0]) ||
        (tls_opts.ca_file && tls_opts.ca_file[0]) ||
        (tls_opts.cert_pem && tls_opts.cert_pem[0]) ||
        (tls_opts.key_pem && tls_opts.key_pem[0]) ||
        (tls_opts.ca_pem && tls_opts.ca_pem[0]) ||
        tls_opts.auth_mode != 0;
    svc->tls_enabled = tls_opts.enabled ? 1 : 0;

    if (svc->running) {
        ducknng_tls_opts_reset(&tls_opts);
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service is already running");
        return -1;
    }
    if (ducknng_transport_url_parse(svc->listen_url, &transport, &parse_err) != 0) {
        ducknng_tls_opts_reset(&tls_opts);
        if (errmsg) *errmsg = parse_err;
        else if (parse_err) duckdb_free(parse_err);
        return -1;
    }
    if (svc->mu_initialized) {
        ducknng_mutex_lock(&svc->mu);
        svc->shutting_down = 0;
        ducknng_mutex_unlock(&svc->mu);
    } else {
        svc->shutting_down = 0;
    }

    if (ducknng_transport_url_is_http(&transport)) {
        char *resolved_url = NULL;
        rv = ducknng_http_server_start(svc, &svc->http_state, &resolved_url, errmsg);
        ducknng_tls_opts_reset(&tls_opts);
        if (rv != 0) return -1;
        if (svc->resolved_listen_url) duckdb_free(svc->resolved_listen_url);
        svc->resolved_listen_url = resolved_url;
        svc->running = 1;
        return 0;
    }

    if (ducknng_listener_validate_startup_url(svc->listen_url, &tls_opts, errmsg) != 0) {
        ducknng_tls_opts_reset(&tls_opts);
        return -1;
    }

    rv = ducknng_rep_socket_open(&svc->rep_sock);
    if (rv != 0) goto fail;
    rv = nng_pipe_notify(svc->rep_sock, NNG_PIPE_EV_ADD_PRE, ducknng_service_pipe_add_pre_cb, svc);
    if (rv != 0) goto fail;
    rv = ducknng_listener_create(&svc->listener, svc->rep_sock, svc->listen_url);
    if (rv != 0) goto fail;
    rv = ducknng_listener_set_recvmaxsz(svc->listener, svc->recv_max_bytes);
    if (rv != 0) goto fail;
    rv = ducknng_listener_apply_tls(svc->listener, tls_opts.enabled ? &tls_opts : NULL);
    if (rv != 0) goto fail;
    rv = ducknng_listener_start(svc->listener);
    if (rv != 0) goto fail;

    svc->resolved_listen_url = ducknng_listener_resolve_url(svc->listener, svc->listen_url);
    svc->ctxs = (ducknng_rep_ctx *)duckdb_malloc(sizeof(*svc->ctxs) * (size_t)svc->ncontexts);
    if (!svc->ctxs) {
        if (errmsg) *errmsg = ducknng_strdup("out of memory");
        rv = NNG_ENOMEM;
        goto fail;
    }
    memset(svc->ctxs, 0, sizeof(*svc->ctxs) * (size_t)svc->ncontexts);

    for (i = 0; i < svc->ncontexts; i++) {
        ducknng_rep_ctx *rc = &svc->ctxs[i];
        rc->svc = svc;
        rv = ducknng_ctx_open(&rc->ctx, svc->rep_sock);
        if (rv != 0) goto fail;
        rv = ducknng_aio_alloc(&rc->aio, ducknng_rep_aio_cb, rc, 30000);
        if (rv != 0) goto fail;
        if (ducknng_mutex_init(&rc->mu) != 0) {
            rv = NNG_EINTERNAL;
            goto fail;
        }
        rc->mu_initialized = 1;
        if (ducknng_cond_init(&rc->cv) != 0) {
            rv = NNG_EINTERNAL;
            goto fail;
        }
        rc->cv_initialized = 1;
        rv = ducknng_thread_create(&rc->thread, ducknng_rep_worker_main, rc);
        if (rv != 0) {
            if (errmsg && !*errmsg) *errmsg = ducknng_strdup("failed to create worker thread");
            goto fail;
        }
        rc->thread_started = 1;
    }

    svc->running = 1;
    ducknng_tls_opts_reset(&tls_opts);
    return 0;

fail:
    ducknng_tls_opts_reset(&tls_opts);
    if (svc->ctxs) {
        for (i = 0; i < svc->ncontexts; i++) {
            ducknng_rep_ctx_teardown(&svc->ctxs[i]);
        }
        duckdb_free(svc->ctxs);
        svc->ctxs = NULL;
    }
    if (svc->listener.id != 0) {
        ducknng_listener_close(svc->listener);
        memset(&svc->listener, 0, sizeof(svc->listener));
    }
    if (svc->rep_sock.id != 0) {
        ducknng_socket_close(svc->rep_sock);
        memset(&svc->rep_sock, 0, sizeof(svc->rep_sock));
    }
    if (svc->resolved_listen_url) {
        duckdb_free(svc->resolved_listen_url);
        svc->resolved_listen_url = NULL;
    }
    if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
    return -1;
}

int ducknng_service_requires_peer_identity(const ducknng_service *svc) {
    return svc && svc->tls_enabled && svc->tls_opts.auth_mode == 2;
}

int ducknng_service_peer_allowlist_active(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->tls_opts.peer_allowlist_active ? 1 : 0;
}

size_t ducknng_service_peer_allowlist_count(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->tls_opts.peer_allowlist_count;
}

int ducknng_service_peer_admission_check(ducknng_service *svc, const char *caller_identity, char **errmsg) {
    int require_identity;
    int allowlist_active;
    int allowed;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing service for peer admission");
        return -1;
    }
    require_identity = ducknng_service_requires_peer_identity(svc);
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    allowlist_active = svc->tls_opts.peer_allowlist_active ? 1 : 0;
    allowed = ducknng_tls_opts_peer_allowed(&svc->tls_opts, caller_identity);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (require_identity && (!caller_identity || !caller_identity[0])) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: mTLS peer identity is required");
        return -1;
    }
    if (allowlist_active && !allowed) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: peer identity is not admitted");
        return -1;
    }
    return 0;
}

int ducknng_service_network_admission_check(ducknng_service *svc, const char *caller_identity,
    const nng_sockaddr *remote_addr, char **errmsg) {
    int rc;
    int ip_active;
    int ip_allowed;
    if (errmsg) *errmsg = NULL;
    rc = ducknng_service_peer_admission_check(svc, caller_identity, errmsg);
    if (rc != 0) return rc;
    if (!svc) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup("ducknng: missing service for network admission");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    ip_active = svc->ip_allowlist_active ? 1 : 0;
    ip_allowed = ducknng_remote_addr_matches_ip_allowlist(svc, remote_addr);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    if (ip_active && !ip_allowed) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: remote address is not admitted");
        return -1;
    }
    return 0;
}

int ducknng_service_set_peer_allowlist(ducknng_service *svc, const char *identities_json, char **errmsg) {
    int rc;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    rc = ducknng_tls_opts_set_peer_allowlist(&svc->tls_opts, identities_json, errmsg);
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return rc;
}

int ducknng_service_set_ip_allowlist(ducknng_service *svc, const char *cidrs_json, char **errmsg) {
    ducknng_ip_allow_rule *rules = NULL;
    size_t count = 0;
    char *json = NULL;
    if (errmsg) *errmsg = NULL;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: service not found");
        return -1;
    }
    if (!cidrs_json || !cidrs_json[0]) {
        if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
        ducknng_service_clear_ip_allowlist(svc);
        if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
        return 0;
    }
    if (ducknng_parse_ip_allowlist_json(cidrs_json, &rules, &count, &json, errmsg) != 0) return -1;
    if (svc->mu_initialized) ducknng_mutex_lock(&svc->mu);
    ducknng_service_clear_ip_allowlist(svc);
    svc->ip_allowlist_active = 1;
    svc->ip_allowlist = rules;
    svc->ip_allowlist_count = count;
    svc->ip_allowlist_json = json;
    if (svc->mu_initialized) ducknng_mutex_unlock(&svc->mu);
    return 0;
}

int ducknng_service_ip_allowlist_active(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->ip_allowlist_active ? 1 : 0;
}

size_t ducknng_service_ip_allowlist_count(const ducknng_service *svc) {
    if (!svc) return 0;
    return svc->ip_allowlist_count;
}

int ducknng_service_stop(ducknng_service *svc, char **errmsg) {
    int i;
    ducknng_session **sessions = NULL;
    size_t session_count = 0;
    size_t j;
    (void)errmsg;
    if (!svc) return -1;
    if (svc->mu_initialized) {
        ducknng_mutex_lock(&svc->mu);
        svc->shutting_down = 1;
        ducknng_mutex_unlock(&svc->mu);
    } else {
        svc->shutting_down = 1;
    }
    if (svc->listener.id != 0) {
        ducknng_listener_close(svc->listener);
        memset(&svc->listener, 0, sizeof(svc->listener));
    }
    if (svc->rep_sock.id != 0) {
        ducknng_socket_close(svc->rep_sock);
        memset(&svc->rep_sock, 0, sizeof(svc->rep_sock));
    }
    if (svc->ctxs) {
        for (i = 0; i < svc->ncontexts; i++) {
            ducknng_rep_ctx_teardown(&svc->ctxs[i]);
        }
        duckdb_free(svc->ctxs);
        svc->ctxs = NULL;
    }
    if (svc->http_state) {
        ducknng_http_server_stop(svc->http_state);
        svc->http_state = NULL;
    }
    sessions = ducknng_service_detach_all_sessions(svc, &session_count);
    if (svc->resolved_listen_url) {
        duckdb_free(svc->resolved_listen_url);
        svc->resolved_listen_url = NULL;
    }
    svc->running = 0;
    for (j = 0; j < session_count; j++) {
        if (sessions && sessions[j]) ducknng_session_destroy(sessions[j]);
    }
    if (sessions) duckdb_free(sessions);
    return 0;
}

const char *ducknng_service_resolved_listen(const ducknng_service *svc) {
    if (!svc) return NULL;
    return svc->resolved_listen_url ? svc->resolved_listen_url : svc->listen_url;
}
