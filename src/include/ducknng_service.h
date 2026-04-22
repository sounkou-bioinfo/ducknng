#pragma once
#include "ducknng_nng_compat.h"
#include "ducknng_session.h"
#include "ducknng_thread.h"
#include "ducknng_wire.h"
#include <stddef.h>
#include <stdint.h>

typedef struct ducknng_runtime ducknng_runtime;
typedef struct ducknng_service ducknng_service;
typedef struct ducknng_rep_ctx ducknng_rep_ctx;

struct ducknng_rep_ctx {
    ducknng_service *svc;
    nng_ctx ctx;
    nng_aio *aio;
    ducknng_thread thread;
    ducknng_mutex mu;
    ducknng_cond cv;
    int stopping;
    int phase;
    int event;
    int last_nng_err;
    int mu_initialized;
    int cv_initialized;
    int thread_started;
    nng_msg *request_msg;
};

struct ducknng_service {
    uint64_t service_id;
    char *name;
    char *listen_url;
    char *resolved_listen_url;
    int tls_enabled;
    uint64_t tls_config_id;
    char *tls_config_source;
    ducknng_tls_opts tls_opts;
    nng_socket rep_sock;
    nng_listener listener;
    ducknng_rep_ctx *ctxs;
    int ncontexts;
    ducknng_mutex mu;
    ducknng_session **sessions;
    size_t session_count;
    size_t session_cap;
    uint64_t next_session_id;
    uint64_t session_idle_ms;
    size_t recv_max_bytes;
    int running;
    int shutting_down;
    ducknng_runtime *rt;
};

enum {
    DUCKNNG_PHASE_RECV = 1,
    DUCKNNG_PHASE_SEND = 2
};

enum {
    DUCKNNG_EVT_NONE = 0,
    DUCKNNG_EVT_REQUEST_READY = 1,
    DUCKNNG_EVT_SEND_DONE = 2,
    DUCKNNG_EVT_NNG_ERROR = 3
};

ducknng_service *ducknng_service_create(ducknng_runtime *rt, const char *name, const char *listen_url,
    int contexts, size_t recv_max_bytes, uint64_t session_idle_ms,
    uint64_t tls_config_id, const char *tls_config_source, const ducknng_tls_opts *tls_opts);
void ducknng_service_destroy(ducknng_service *svc);
int ducknng_service_start(ducknng_service *svc, char **errmsg);
int ducknng_service_stop(ducknng_service *svc, char **errmsg);
nng_msg *ducknng_handle_request(ducknng_service *svc, nng_msg *req);
const char *ducknng_service_resolved_listen(const ducknng_service *svc);
