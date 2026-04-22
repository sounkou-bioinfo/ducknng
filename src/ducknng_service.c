#include "ducknng_service.h"
#include "ducknng_registry.h"
#include "ducknng_runtime.h"
#include "ducknng_util.h"
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

static nng_msg *ducknng_dispatch_request(ducknng_service *svc, const ducknng_frame *frame) {
    const ducknng_method_descriptor *method = NULL;
    ducknng_request_context req_ctx;
    ducknng_method_reply reply;
    nng_msg *msg;

    memset(&req_ctx, 0, sizeof(req_ctx));
    ducknng_method_reply_init(&reply);

    if (!svc || !svc->rt || !frame) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INTERNAL,
            "ducknng: missing dispatcher state");
    }
    ducknng_service_prune_idle_sessions(svc, ducknng_now_ms());
    if (frame->type == DUCKNNG_RPC_MANIFEST) {
        method = ducknng_method_registry_find(&svc->rt->registry,
            (const uint8_t *)"manifest", (uint32_t)strlen("manifest"));
    } else if (frame->type == DUCKNNG_RPC_CALL) {
        method = ducknng_method_registry_find(&svc->rt->registry, frame->name, frame->name_len);
    } else {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INVALID,
            "ducknng: unsupported message type");
    }
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
    if (method->requires_auth) {
        return ducknng_error_msg(method->name, DUCKNNG_STATUS_UNAUTHORIZED,
            "ducknng: authenticated methods are not implemented yet");
    }

    req_ctx.frame = frame;
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
    if (rc->thread_started) {
        ducknng_thread_join(rc->thread);
        rc->thread_started = 0;
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

nng_msg *ducknng_handle_request(ducknng_service *svc, nng_msg *req) {
    ducknng_frame frame;
    if (ducknng_decode_request(req, &frame) != 0) {
        return ducknng_error_msg(NULL, DUCKNNG_STATUS_INVALID, "invalid RPC envelope");
    }
    return ducknng_dispatch_request(svc, &frame);
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
    ducknng_mutex_init(&svc->mu);
    return svc;
}

void ducknng_service_destroy(ducknng_service *svc) {
    if (!svc) return;
    if (svc->name) duckdb_free(svc->name);
    if (svc->listen_url) duckdb_free(svc->listen_url);
    if (svc->resolved_listen_url) duckdb_free(svc->resolved_listen_url);
    if (svc->tls_config_source) duckdb_free(svc->tls_config_source);
    ducknng_tls_opts_reset(&svc->tls_opts);
    if (svc->sessions) {
        size_t i;
        for (i = 0; i < svc->session_count; i++) {
            if (svc->sessions[i]) ducknng_session_destroy(svc->sessions[i]);
        }
        duckdb_free(svc->sessions);
    }
    if (svc->ctxs) duckdb_free(svc->ctxs);
    ducknng_mutex_destroy(&svc->mu);
    duckdb_free(svc);
}

int ducknng_service_start(ducknng_service *svc, char **errmsg) {
    int rv;
    int i;
    ducknng_tls_opts tls_opts;
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
    if (ducknng_listener_validate_startup_url(svc->listen_url, &tls_opts, errmsg) != 0) {
        ducknng_tls_opts_reset(&tls_opts);
        return -1;
    }

    rv = ducknng_rep_socket_open(&svc->rep_sock);
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

int ducknng_service_stop(ducknng_service *svc, char **errmsg) {
    int i;
    (void)errmsg;
    if (!svc) return -1;
    svc->shutting_down = 1;
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
    }
    svc->running = 0;
    if (svc->sessions) {
        size_t i;
        for (i = 0; i < svc->session_count; i++) {
            if (svc->sessions[i]) ducknng_session_destroy(svc->sessions[i]);
        }
        duckdb_free(svc->sessions);
        svc->sessions = NULL;
        svc->session_count = 0;
        svc->session_cap = 0;
    }
    return 0;
}

const char *ducknng_service_resolved_listen(const ducknng_service *svc) {
    if (!svc) return NULL;
    return svc->resolved_listen_url ? svc->resolved_listen_url : svc->listen_url;
}
