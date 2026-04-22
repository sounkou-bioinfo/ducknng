#include "ducknng_session.h"
#include "ducknng_service.h"
#include "ducknng_util.h"
#include <string.h>

DUCKDB_EXTENSION_EXTERN

ducknng_session *ducknng_session_create(duckdb_result *result, uint64_t session_id, char **errmsg) {
    ducknng_session *session = (ducknng_session *)duckdb_malloc(sizeof(*session));
    if (!session) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory allocating session");
        if (result) duckdb_destroy_result(result);
        return NULL;
    }
    memset(session, 0, sizeof(*session));
    session->session_id = session_id;
    if (result) {
        session->result = *result;
        memset(result, 0, sizeof(*result));
        session->result_open = 1;
    }
    session->last_touch_ms = ducknng_now_ms();
    ducknng_mutex_init(&session->mu);
    return session;
}

void ducknng_session_destroy(ducknng_session *session) {
    if (!session) return;
    if (session->result_open) {
        duckdb_destroy_result(&session->result);
        session->result_open = 0;
    }
    ducknng_mutex_destroy(&session->mu);
    duckdb_free(session);
}

ducknng_session *ducknng_service_add_session(ducknng_service *svc, duckdb_result *result, char **errmsg) {
    ducknng_session **new_sessions;
    size_t new_cap;
    ducknng_session *session;
    if (!svc) {
        if (errmsg) *errmsg = ducknng_strdup("ducknng: missing service for session add");
        if (result) duckdb_destroy_result(result);
        return NULL;
    }
    ducknng_mutex_lock(&svc->mu);
    if (svc->session_count == svc->session_cap) {
        new_cap = svc->session_cap ? svc->session_cap * 2 : 4;
        new_sessions = (ducknng_session **)duckdb_malloc(sizeof(*new_sessions) * new_cap);
        if (!new_sessions) {
            ducknng_mutex_unlock(&svc->mu);
            if (errmsg) *errmsg = ducknng_strdup("ducknng: out of memory growing session table");
            if (result) duckdb_destroy_result(result);
            return NULL;
        }
        memset(new_sessions, 0, sizeof(*new_sessions) * new_cap);
        if (svc->sessions && svc->session_count) {
            memcpy(new_sessions, svc->sessions, sizeof(*new_sessions) * svc->session_count);
        }
        if (svc->sessions) duckdb_free(svc->sessions);
        svc->sessions = new_sessions;
        svc->session_cap = new_cap;
    }
    session = ducknng_session_create(result, svc->next_session_id++, errmsg);
    if (!session) {
        ducknng_mutex_unlock(&svc->mu);
        return NULL;
    }
    svc->sessions[svc->session_count++] = session;
    ducknng_mutex_unlock(&svc->mu);
    return session;
}

ducknng_session *ducknng_service_find_session(ducknng_service *svc, uint64_t session_id) {
    size_t i;
    ducknng_session *session = NULL;
    if (!svc || session_id == 0) return NULL;
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->session_count; i++) {
        if (svc->sessions[i] && svc->sessions[i]->session_id == session_id) {
            session = svc->sessions[i];
            break;
        }
    }
    ducknng_mutex_unlock(&svc->mu);
    return session;
}

ducknng_session *ducknng_service_remove_session(ducknng_service *svc, uint64_t session_id) {
    size_t i;
    ducknng_session *session = NULL;
    if (!svc || session_id == 0) return NULL;
    ducknng_mutex_lock(&svc->mu);
    for (i = 0; i < svc->session_count; i++) {
        if (svc->sessions[i] && svc->sessions[i]->session_id == session_id) {
            session = svc->sessions[i];
            for (; i + 1 < svc->session_count; i++) svc->sessions[i] = svc->sessions[i + 1];
            svc->session_count--;
            break;
        }
    }
    ducknng_mutex_unlock(&svc->mu);
    return session;
}

size_t ducknng_service_prune_idle_sessions(ducknng_service *svc, uint64_t now_ms) {
    size_t i = 0;
    size_t removed = 0;
    if (!svc || svc->session_idle_ms == 0) return 0;
    ducknng_mutex_lock(&svc->mu);
    while (i < svc->session_count) {
        ducknng_session *session = svc->sessions[i];
        if (session && !session->cancelled && !session->eos && now_ms >= session->last_touch_ms &&
            (now_ms - session->last_touch_ms) > svc->session_idle_ms) {
            for (; i + 1 < svc->session_count; i++) svc->sessions[i] = svc->sessions[i + 1];
            svc->session_count--;
            removed++;
            ducknng_mutex_unlock(&svc->mu);
            ducknng_session_destroy(session);
            ducknng_mutex_lock(&svc->mu);
            continue;
        }
        i++;
    }
    ducknng_mutex_unlock(&svc->mu);
    return removed;
}
