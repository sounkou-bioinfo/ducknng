#include "ducknng_nng_compat.h"
#include "ducknng_util.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *ducknng_url_with_port(const nng_url *up, int port) {
    char port_buf[32];
    size_t need;
    char *out;
    if (!up || !up->u_scheme || !up->u_hostname) return NULL;
    snprintf(port_buf, sizeof(port_buf), "%d", port);
    need = strlen(up->u_scheme) + strlen(up->u_hostname) + strlen(port_buf) + 8;
    if (up->u_path) need += strlen(up->u_path);
    out = (char *)malloc(need);
    if (!out) return NULL;
    snprintf(out, need, "%s://%s:%s%s", up->u_scheme, up->u_hostname, port_buf, up->u_path ? up->u_path : "");
    return out;
}

static int ducknng_tls_requested(const ducknng_tls_opts *opts) {
    return opts && (opts->enabled ||
        (opts->cert_key_file && opts->cert_key_file[0]) ||
        (opts->ca_file && opts->ca_file[0]) || opts->auth_mode != 0);
}

static const char *ducknng_normalize_host(const char *host, size_t *len_out) {
    size_t len;
    if (!host) {
        if (len_out) *len_out = 0;
        return NULL;
    }
    len = strlen(host);
    if (len >= 2 && host[0] == '[' && host[len - 1] == ']') {
        if (len_out) *len_out = len - 2;
        return host + 1;
    }
    if (len_out) *len_out = len;
    return host;
}

static int ducknng_host_is_loopback(const char *host) {
    const char *norm;
    size_t len;
    norm = ducknng_normalize_host(host, &len);
    if (!norm || len == 0) return 0;
    if (len == strlen("localhost") && strncmp(norm, "localhost", len) == 0) return 1;
    if (len == strlen("127.0.0.1") && strncmp(norm, "127.0.0.1", len) == 0) return 1;
    if (len == strlen("::1") && strncmp(norm, "::1", len) == 0) return 1;
    return 0;
}

int ducknng_rep_socket_open(nng_socket *out) { return nng_rep0_open(out); }
int ducknng_req_socket_open(nng_socket *out) { return nng_req0_open(out); }
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
int ducknng_ctx_send(nng_ctx ctx, nng_msg *msg) { return nng_ctx_sendmsg(ctx, msg, 0); }
int ducknng_ctx_recv(nng_ctx ctx, nng_msg **msg) { return nng_ctx_recvmsg(ctx, msg, 0); }
int ducknng_req_dial(nng_socket sock, const char *url, int timeout_ms) {
    int rv = ducknng_socket_set_timeout_ms(sock, timeout_ms, timeout_ms);
    if (rv != 0) return rv;
    return ducknng_socket_dial(sock, url);
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
        if (tls_requested) {
            if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS listener options are not implemented; refusing tls+tcp startup");
        } else if (errmsg) {
            *errmsg = ducknng_strdup("ducknng: tls+tcp listeners require TLS support, which is not implemented");
        }
        rc = -1;
        goto done;
    }
    if (tls_requested) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: TLS listener options are not implemented; refusing to start listener");
        rc = -1;
        goto done;
    }
    if (strcmp(up->u_scheme, "tcp") == 0 && !ducknng_host_is_loopback(up->u_hostname)) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: refusing unauthenticated non-loopback tcp listener");
        rc = -1;
        goto done;
    }

done:
    if (up) nng_url_free(up);
    return rc;
}
int ducknng_listener_set_recvmaxsz(nng_listener lst, size_t bytes) { return nng_listener_set_size(lst, NNG_OPT_RECVMAXSZ, bytes); }
int ducknng_listener_apply_tls(nng_listener lst, const ducknng_tls_opts *opts) {
    (void)lst;
    if (ducknng_tls_requested(opts)) return NNG_ENOTSUP;
    return 0;
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
int ducknng_aio_alloc(nng_aio **out, void (*cb)(void *), void *arg, int timeout_ms) {
    int rv = nng_aio_alloc(out, cb, arg);
    if (rv == 0 && timeout_ms > 0) {
        nng_aio_set_timeout(*out, timeout_ms);
    }
    return rv;
}
void ducknng_aio_free(nng_aio *aio) { nng_aio_free(aio); }
const char *ducknng_nng_strerror(int err) { return nng_strerror(err); }
