#pragma once
#include "ducknng_nng_compat.h"
#include <stddef.h>
#include <stdint.h>

int ducknng_validate_http_url(const char *url, char **errmsg);
int ducknng_http_transact(const char *url, const char *method, const char *headers_json,
    const uint8_t *body, size_t body_len, int timeout_ms, const ducknng_tls_opts *tls_opts,
    uint16_t *out_status, char **out_headers_json, uint8_t **out_body, size_t *out_body_len,
    char **errmsg);
