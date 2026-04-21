#pragma once
#include <stddef.h>
#include <stdint.h>

typedef struct ducknng_exec_request {
    char *sql;
    int want_result;
} ducknng_exec_request;

int ducknng_decode_exec_request_payload(const uint8_t *payload, size_t payload_len,
    ducknng_exec_request *out, char **errmsg);
int ducknng_decode_exec_metadata_payload(const uint8_t *payload, size_t payload_len,
    uint64_t *rows_changed, uint32_t *statement_type, uint32_t *result_type, char **errmsg);
void ducknng_exec_request_destroy(ducknng_exec_request *req);
