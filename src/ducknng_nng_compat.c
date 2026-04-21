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

int ducknng_rep_socket_open(nng_socket *out) { return nng_rep0_open(out); }
int ducknng_req_socket_open(nng_socket *out) { return nng_req0_open(out); }
int ducknng_listener_create(nng_listener *out, nng_socket sock, const char *url) { return nng_listener_create(out, sock, url); }
int ducknng_listener_set_recvmaxsz(nng_listener lst, size_t bytes) { return nng_listener_setopt_size(lst, NNG_OPT_RECVMAXSZ, bytes); }
int ducknng_listener_apply_tls(nng_listener lst, const ducknng_tls_opts *opts) {
    (void)lst;
    if (opts && opts->enabled) return NNG_ENOTSUP;
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
