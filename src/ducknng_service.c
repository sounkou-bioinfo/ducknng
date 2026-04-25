#include "ducknng_service.h"
#include "ducknng_registry.h"
#include "ducknng_runtime.h"
#include "ducknng_transport.h"
#include "ducknng_util.h"
#include <stdlib.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

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
    char *caller_identity = ducknng_msg_verified_peer_identity(req);
    nng_msg *reply = ducknng_handle_request_with_identity(svc, req, caller_identity);
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
    int allowed = 1;
    if (ev != NNG_PIPE_EV_ADD_PRE || !svc) return;
    identity = ducknng_pipe_verified_peer_identity(pipe);
    allowed = ducknng_service_peer_admission_check(svc, identity, NULL) == 0;
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
