#pragma once
#include <stddef.h>
#include <stdint.h>
#include <nng/nng.h>
#include <nng/protocol/reqrep0/req.h>
#include <nng/protocol/reqrep0/rep.h>

typedef struct ducknng_tls_opts {
    int enabled;
    char *cert_key_file;
    char *ca_file;
    int auth_mode;
} ducknng_tls_opts;

int ducknng_rep_socket_open(nng_socket *out);
int ducknng_req_socket_open(nng_socket *out);
int ducknng_listener_create(nng_listener *out, nng_socket sock, const char *url);
int ducknng_listener_set_recvmaxsz(nng_listener lst, size_t bytes);
int ducknng_listener_apply_tls(nng_listener lst, const ducknng_tls_opts *opts);
int ducknng_listener_start(nng_listener lst);
int ducknng_listener_close(nng_listener lst);
char *ducknng_listener_resolve_url(nng_listener lst, const char *url);
int ducknng_socket_close(nng_socket sock);
int ducknng_ctx_open(nng_ctx *out, nng_socket sock);
int ducknng_ctx_close(nng_ctx ctx);
int ducknng_aio_alloc(nng_aio **out, void (*cb)(void *), void *arg, int timeout_ms);
void ducknng_aio_free(nng_aio *aio);
const char *ducknng_nng_strerror(int err);
