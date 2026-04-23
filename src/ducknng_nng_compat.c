#include "ducknng_nng_compat.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <nng/transport/tls/tls.h>

DUCKDB_EXTENSION_EXTERN

void ducknng_tls_opts_init(ducknng_tls_opts *opts) {
    if (!opts) return;
    memset(opts, 0, sizeof(*opts));
}

int ducknng_tls_opts_copy(ducknng_tls_opts *dst, const ducknng_tls_opts *src) {
    if (!dst) return -1;
    ducknng_tls_opts_init(dst);
    if (!src) return 0;
    dst->enabled = src->enabled;
    dst->auth_mode = src->auth_mode;
    dst->cert_key_file = ducknng_strdup(src->cert_key_file);
    dst->ca_file = ducknng_strdup(src->ca_file);
    dst->cert_pem = ducknng_strdup(src->cert_pem);
    dst->key_pem = ducknng_strdup(src->key_pem);
    dst->ca_pem = ducknng_strdup(src->ca_pem);
    dst->password = ducknng_strdup(src->password);
    if ((src->cert_key_file && !dst->cert_key_file) ||
        (src->ca_file && !dst->ca_file) ||
        (src->cert_pem && !dst->cert_pem) ||
        (src->key_pem && !dst->key_pem) ||
        (src->ca_pem && !dst->ca_pem) ||
        (src->password && !dst->password)) {
        ducknng_tls_opts_reset(dst);
        return -1;
    }
    return 0;
}

void ducknng_tls_opts_reset(ducknng_tls_opts *opts) {
    if (!opts) return;
    if (opts->cert_key_file) duckdb_free(opts->cert_key_file);
    if (opts->ca_file) duckdb_free(opts->ca_file);
    if (opts->cert_pem) duckdb_free(opts->cert_pem);
    if (opts->key_pem) duckdb_free(opts->key_pem);
    if (opts->ca_pem) duckdb_free(opts->ca_pem);
    if (opts->password) duckdb_free(opts->password);
    memset(opts, 0, sizeof(*opts));
}

static char *ducknng_url_with_port(const nng_url *up, int port) {
    char port_buf[32];
    size_t need;
    char *out;
    if (!up || !up->u_scheme || !up->u_hostname) return NULL;
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    need = strlen(up->u_scheme) + strlen(up->u_hostname) + strlen(port_buf) + 8;
    if (up->u_path) need += strlen(up->u_path);
    out = (char *)duckdb_malloc(need);
    if (!out) return NULL;
    snprintf(out, need, "%s://%s:%s%s", up->u_scheme, up->u_hostname, port_buf, up->u_path ? up->u_path : "");
    return out;
}

static int ducknng_tls_requested(const ducknng_tls_opts *opts) {
    return opts && (opts->enabled ||
        (opts->cert_key_file && opts->cert_key_file[0]) ||
        (opts->ca_file && opts->ca_file[0]) ||
        (opts->cert_pem && opts->cert_pem[0]) ||
        (opts->key_pem && opts->key_pem[0]) ||
        (opts->ca_pem && opts->ca_pem[0]) ||
        (opts->password && opts->password[0]) ||
        opts->auth_mode != 0);
}

static int ducknng_tls_auth_mode_map(int auth_mode, nng_tls_auth_mode *out) {
    if (!out) return NNG_EINVAL;
    switch (auth_mode) {
    case 0: *out = NNG_TLS_AUTH_MODE_NONE; return 0;
    case 1: *out = NNG_TLS_AUTH_MODE_OPTIONAL; return 0;
    case 2: *out = NNG_TLS_AUTH_MODE_REQUIRED; return 0;
    default: return NNG_EINVAL;
    }
}

static int ducknng_tls_config_build(nng_tls_config **out, nng_tls_mode mode, const char *url,
    const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    nng_tls_auth_mode auth_mode;
    int rv;
    nng_url *up = NULL;
    if (!out) return NNG_EINVAL;
    *out = NULL;
    if (!opts || !ducknng_tls_requested(opts)) return 0;
    rv = nng_tls_config_alloc(&cfg, mode);
    if (rv != 0) return rv;
    rv = ducknng_tls_auth_mode_map(opts->auth_mode, &auth_mode);
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
    if (mode == NNG_TLS_MODE_CLIENT && url && nng_url_parse(&up, url) == 0 && up && up->u_hostname && up->u_hostname[0]) {
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

int ducknng_rep_socket_open(nng_socket *out) { return nng_rep0_open(out); }
int ducknng_req_socket_open(nng_socket *out) { return nng_req0_open(out); }
int ducknng_socket_open_protocol(const char *protocol, nng_socket *out, char **errmsg) {
    int rv = NNG_EINVAL;
    if (errmsg) *errmsg = NULL;
    if (!protocol || !protocol[0] || !out) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: socket protocol is required");
        return -1;
    }
    if (strcmp(protocol, "bus") == 0) rv = nng_bus0_open(out);
    else if (strcmp(protocol, "pair") == 0 || strcmp(protocol, "pair0") == 0) rv = nng_pair0_open(out);
    else if (strcmp(protocol, "poly") == 0) rv = nng_pair1_open_poly(out);
    else if (strcmp(protocol, "pair1") == 0) rv = nng_pair1_open(out);
    else if (strcmp(protocol, "push") == 0) rv = nng_push0_open(out);
    else if (strcmp(protocol, "pull") == 0) rv = nng_pull0_open(out);
    else if (strcmp(protocol, "pub") == 0) rv = nng_pub0_open(out);
    else if (strcmp(protocol, "sub") == 0) rv = nng_sub0_open(out);
    else if (strcmp(protocol, "req") == 0) rv = nng_req0_open(out);
    else if (strcmp(protocol, "rep") == 0) rv = nng_rep0_open(out);
    else if (strcmp(protocol, "surveyor") == 0) rv = nng_surveyor0_open(out);
    else if (strcmp(protocol, "respondent") == 0) rv = nng_respondent0_open(out);
    else {
        if (errmsg) *errmsg = ducknng_strdup(
            "ducknng: protocol must be one of bus, pair, pair0, pair1, poly, push, pull, pub, sub, req, rep, surveyor, respondent");
        return -1;
    }
    if (rv != 0) {
        if (errmsg && !*errmsg) *errmsg = ducknng_strdup(ducknng_nng_strerror(rv));
        return -1;
    }
    return 0;
}
int ducknng_socket_set_timeout_ms(nng_socket sock, int send_timeout_ms, int recv_timeout_ms) {
    int rv;
    if (send_timeout_ms > 0) {
        rv = nng_socket_set_ms(sock, NNG_OPT_SENDTIMEO, send_timeout_ms);
        if (rv != 0) return rv;
    }
    if (recv_timeout_ms > 0) {
        rv = nng_socket_set_ms(sock, NNG_OPT_RECVTIMEO, recv_timeout_ms);
        if (rv != 0) return rv;
    }
    return 0;
}
int ducknng_socket_dial(nng_socket sock, const char *url) { return nng_dial(sock, url, NULL, 0); }
int ducknng_socket_send(nng_socket sock, nng_msg *msg) { return nng_sendmsg(sock, msg, 0); }
int ducknng_socket_recv(nng_socket sock, nng_msg **msg) { return nng_recvmsg(sock, msg, 0); }
int ducknng_socket_subscribe(nng_socket sock, const void *topic, size_t len) {
    return nng_socket_set(sock, NNG_OPT_SUB_SUBSCRIBE, topic, len);
}
int ducknng_socket_unsubscribe(nng_socket sock, const void *topic, size_t len) {
    return nng_socket_set(sock, NNG_OPT_SUB_UNSUBSCRIBE, topic, len);
}
int ducknng_ctx_send(nng_ctx ctx, nng_msg *msg) { return nng_ctx_sendmsg(ctx, msg, 0); }
int ducknng_ctx_recv(nng_ctx ctx, nng_msg **msg) { return nng_ctx_recvmsg(ctx, msg, 0); }
int ducknng_req_dial(nng_socket sock, const char *url, int timeout_ms) {
    int rv = ducknng_socket_set_timeout_ms(sock, timeout_ms, timeout_ms);
    if (rv != 0) return rv;
    return ducknng_socket_dial(sock, url);
}
int ducknng_socket_apply_tls(nng_socket sock, const char *url, const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    int rv = ducknng_tls_config_build(&cfg, NNG_TLS_MODE_CLIENT, url, opts);
    if (rv != 0) return rv;
    if (!cfg) return 0;
    rv = nng_socket_set_ptr(sock, NNG_OPT_TLS_CONFIG, cfg);
    nng_tls_config_free(cfg);
    return rv;
}
int ducknng_req_transact(nng_socket sock, nng_msg *req, nng_msg **resp) {
    int rv;
    rv = ducknng_socket_send(sock, req);
    if (rv != 0) return rv;
    return ducknng_socket_recv(sock, resp);
}
int ducknng_listener_create(nng_listener *out, nng_socket sock, const char *url) { return nng_listener_create(out, sock, url); }
int ducknng_listener_validate_startup_url(const char *url, const ducknng_tls_opts *opts, char **errmsg) {
    nng_url *up = NULL;
    int tls_requested = ducknng_tls_requested(opts);
    int rc = 0;
    if (!url || !url[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: listen URL is required");
        return -1;
    }
    if (nng_url_parse(&up, url) != 0 || !up || !up->u_scheme || !up->u_scheme[0]) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: invalid listen URL");
        rc = -1;
        goto done;
    }
    if (strcmp(up->u_scheme, "tls+tcp") == 0) {
        if (!tls_requested) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: tls+tcp listeners require TLS configuration");
            rc = -1;
            goto done;
        }
    } else if (tls_requested) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS configuration requires a tls+tcp:// listen URL");
        rc = -1;
        goto done;
    }

done:
    if (up) nng_url_free(up);
    return rc;
}
int ducknng_listener_set_recvmaxsz(nng_listener lst, size_t bytes) { return nng_listener_set_size(lst, NNG_OPT_RECVMAXSZ, bytes); }
int ducknng_listener_apply_tls(nng_listener lst, const ducknng_tls_opts *opts) {
    nng_tls_config *cfg = NULL;
    int rv = ducknng_tls_config_build(&cfg, NNG_TLS_MODE_SERVER, NULL, opts);
    if (rv != 0) return rv;
    if (!cfg) return 0;
    rv = nng_listener_set_ptr(lst, NNG_OPT_TLS_CONFIG, cfg);
    nng_tls_config_free(cfg);
    return rv;
}
int ducknng_listener_start(nng_listener lst) { return nng_listener_start(lst, 0); }
int ducknng_listener_close(nng_listener lst) { return nng_listener_close(lst); }
char *ducknng_listener_resolve_url(nng_listener lst, const char *url) {
    nng_url *up = NULL;
    char *resolved = NULL;
    int port = 0;
    if (!url) return NULL;
    if (nng_url_parse(&up, url) != 0 || !up) goto done;
    if (!up->u_port || strcmp(up->u_port, "0") != 0) goto done;
    if (strcmp(up->u_scheme, "tcp") != 0 && strcmp(up->u_scheme, "tls+tcp") != 0) goto done;
    if (nng_listener_get_int(lst, NNG_OPT_TCP_BOUND_PORT, &port) != 0 || port <= 0) goto done;
    resolved = ducknng_url_with_port(up, port);
done:
    if (up) nng_url_free(up);
    return resolved;
}
int ducknng_socket_close(nng_socket sock) { return nng_close(sock); }
int ducknng_ctx_open(nng_ctx *out, nng_socket sock) { return nng_ctx_open(out, sock); }
int ducknng_ctx_close(nng_ctx ctx) { return nng_ctx_close(ctx); }
void ducknng_ctx_recv_aio(nng_ctx ctx, nng_aio *aio) { nng_ctx_recv(ctx, aio); }
void ducknng_ctx_send_aio(nng_ctx ctx, nng_aio *aio) { nng_ctx_send(ctx, aio); }
void ducknng_socket_recv_aio(nng_socket sock, nng_aio *aio) { nng_recv_aio(sock, aio); }
void ducknng_socket_send_aio(nng_socket sock, nng_aio *aio) { nng_send_aio(sock, aio); }
int ducknng_aio_alloc(nng_aio **out, void (*cb)(void *), void *arg, int timeout_ms) {
    int rv = nng_aio_alloc(out, cb, arg);
    if (rv == 0 && timeout_ms > 0) {
        nng_aio_set_timeout(*out, timeout_ms);
    }
    return rv;
}
void ducknng_aio_free(nng_aio *aio) { nng_aio_free(aio); }
int ducknng_aio_result(nng_aio *aio) { return nng_aio_result(aio); }
void ducknng_aio_cancel(nng_aio *aio) { nng_aio_cancel(aio); }
void ducknng_aio_wait(nng_aio *aio) { nng_aio_wait(aio); }
void ducknng_aio_set_msg(nng_aio *aio, nng_msg *msg) { nng_aio_set_msg(aio, msg); }
nng_msg *ducknng_aio_get_msg(nng_aio *aio) { return nng_aio_get_msg(aio); }
const char *ducknng_nng_strerror(int err) { return nng_strerror(err); }
