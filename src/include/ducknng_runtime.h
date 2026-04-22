#pragma once
#include "duckdb_extension.h"
#include "ducknng_service.h"
#include "ducknng_thread.h"
#include "ducknng_registry.h"
#include <stddef.h>
#include <stdint.h>

typedef struct ducknng_client_socket {
    uint64_t socket_id;
    char *protocol;
    char *url;
    nng_socket sock;
    nng_ctx ctx;
    int open;
    int connected;
    int has_ctx;
    int send_timeout_ms;
    int recv_timeout_ms;
    uint8_t *pending_request;
    size_t pending_request_len;
    uint8_t *pending_reply;
    size_t pending_reply_len;
} ducknng_client_socket;

typedef struct ducknng_tls_config {
    uint64_t tls_config_id;
    char *source;
    ducknng_tls_opts opts;
} ducknng_tls_config;

typedef struct ducknng_runtime {
    duckdb_database *db;
    duckdb_connection init_con;
    ducknng_mutex mu;
    ducknng_service **services;
    size_t service_count;
    size_t service_cap;
    ducknng_client_socket **client_sockets;
    size_t client_socket_count;
    size_t client_socket_cap;
    ducknng_tls_config **tls_configs;
    size_t tls_config_count;
    size_t tls_config_cap;
    uint64_t next_service_id;
    uint64_t next_client_socket_id;
    uint64_t next_tls_config_id;
    int shutting_down;
    ducknng_method_registry registry;
} ducknng_runtime;

int ducknng_runtime_init(duckdb_connection connection, duckdb_extension_info info,
    struct duckdb_extension_access *access, ducknng_runtime **out_rt);
void ducknng_runtime_destroy(ducknng_runtime *rt);
ducknng_service *ducknng_runtime_find_service(ducknng_runtime *rt, const char *name);
int ducknng_runtime_add_service(ducknng_runtime *rt, ducknng_service *svc, char **errmsg);
ducknng_service *ducknng_runtime_remove_service(ducknng_runtime *rt, const char *name);
ducknng_client_socket *ducknng_runtime_find_client_socket(ducknng_runtime *rt, uint64_t socket_id);
int ducknng_runtime_add_client_socket(ducknng_runtime *rt, ducknng_client_socket *sock, char **errmsg);
ducknng_client_socket *ducknng_runtime_remove_client_socket(ducknng_runtime *rt, uint64_t socket_id);
ducknng_tls_config *ducknng_runtime_find_tls_config(ducknng_runtime *rt, uint64_t tls_config_id);
int ducknng_runtime_add_tls_config(ducknng_runtime *rt, ducknng_tls_config *cfg, char **errmsg);
ducknng_tls_config *ducknng_runtime_remove_tls_config(ducknng_runtime *rt, uint64_t tls_config_id);
