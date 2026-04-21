#include "ducknng_runtime.h"
#include "ducknng_manifest.h"
#include "ducknng_util.h"
#include <stdatomic.h>
#include <string.h>

DUCKDB_EXTENSION_EXTERN

typedef struct {
    duckdb_database *db;
    ducknng_runtime *rt;
} ducknng_registry_entry;

static ducknng_registry_entry *g_entries = NULL;
static size_t g_entry_count = 0;
static size_t g_entry_cap = 0;
static atomic_flag g_registry_lock = ATOMIC_FLAG_INIT;

static void reg_lock(void) { while (atomic_flag_test_and_set_explicit(&g_registry_lock, memory_order_acquire)) {} }
static void reg_unlock(void) { atomic_flag_clear_explicit(&g_registry_lock, memory_order_release); }
static long reg_find(duckdb_database *db) {
    size_t i;
    for (i = 0; i < g_entry_count; i++) if (g_entries[i].db == db) return (long)i;
    return -1;
}
static int reg_reserve(size_t want) {
    ducknng_registry_entry *new_entries;
    size_t new_cap = g_entry_cap ? g_entry_cap * 2 : 4;
    if (g_entry_cap >= want) return 1;
    while (new_cap < want) new_cap *= 2;
    new_entries = (ducknng_registry_entry *)duckdb_malloc(sizeof(*new_entries) * new_cap);
    if (!new_entries) return 0;
    memset(new_entries, 0, sizeof(*new_entries) * new_cap);
    if (g_entries && g_entry_count) memcpy(new_entries, g_entries, sizeof(*new_entries) * g_entry_count);
    if (g_entries) duckdb_free(g_entries);
    g_entries = new_entries;
    g_entry_cap = new_cap;
    return 1;
}

int ducknng_runtime_init(duckdb_connection connection, duckdb_extension_info info,
    struct duckdb_extension_access *access, ducknng_runtime **out_rt) {
    duckdb_database *db = NULL;
    ducknng_runtime *rt = NULL;
    long idx;
    char *errmsg = NULL;
    if (!access || !info || !out_rt) return 0;
    db = access->get_database(info);
    if (!db || !*db) {
        access->set_error(info, "ducknng: missing database handle");
        return 0;
    }
    reg_lock();
    idx = reg_find(db);
    if (idx >= 0) {
        *out_rt = g_entries[idx].rt;
        reg_unlock();
        return 1;
    }
    if (!reg_reserve(g_entry_count + 1)) {
        reg_unlock();
        access->set_error(info, "ducknng: failed to grow runtime registry");
        return 0;
    }
    rt = (ducknng_runtime *)duckdb_malloc(sizeof(*rt));
    if (!rt) {
        reg_unlock();
        access->set_error(info, "ducknng: out of memory");
        return 0;
    }
    memset(rt, 0, sizeof(*rt));
    rt->db = db;
    rt->init_con = connection;
    rt->next_service_id = 1;
    ducknng_mutex_init(&rt->mu);
    ducknng_method_registry_init(&rt->registry);
    if (!ducknng_register_builtin_methods(rt, &errmsg)) {
        ducknng_method_registry_destroy(&rt->registry);
        ducknng_mutex_destroy(&rt->mu);
        duckdb_free(rt);
        reg_unlock();
        access->set_error(info, errmsg ? errmsg : "ducknng: failed to register builtin methods");
        if (errmsg) duckdb_free(errmsg);
        return 0;
    }
    g_entries[g_entry_count].db = db;
    g_entries[g_entry_count].rt = rt;
    g_entry_count++;
    *out_rt = rt;
    reg_unlock();
    return 1;
}

void ducknng_runtime_destroy(ducknng_runtime *rt) {
    if (!rt) return;
    ducknng_method_registry_destroy(&rt->registry);
}

ducknng_service *ducknng_runtime_find_service(ducknng_runtime *rt, const char *name) {
    size_t i;
    ducknng_service *svc = NULL;
    if (!rt || !name) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->service_count; i++) {
        if (rt->services[i] && rt->services[i]->name && strcmp(rt->services[i]->name, name) == 0) {
            svc = rt->services[i];
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return svc;
}

int ducknng_runtime_add_service(ducknng_runtime *rt, ducknng_service *svc, char **errmsg) {
    ducknng_service **new_services;
    size_t new_cap;
    size_t i;
    if (!rt || !svc) return -1;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->service_count; i++) {
        if (rt->services[i] && strcmp(rt->services[i]->name, svc->name) == 0) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("service already exists");
            return -1;
        }
    }
    if (rt->service_count == rt->service_cap) {
        new_cap = rt->service_cap ? rt->service_cap * 2 : 4;
        new_services = (ducknng_service **)duckdb_malloc(sizeof(*new_services) * new_cap);
        if (!new_services) {
            ducknng_mutex_unlock(&rt->mu);
            if (errmsg) *errmsg = ducknng_strdup("out of memory");
            return -1;
        }
        memset(new_services, 0, sizeof(*new_services) * new_cap);
        if (rt->services && rt->service_count) memcpy(new_services, rt->services, sizeof(*new_services) * rt->service_count);
        if (rt->services) duckdb_free(rt->services);
        rt->services = new_services;
        rt->service_cap = new_cap;
    }
    svc->service_id = rt->next_service_id++;
    rt->services[rt->service_count++] = svc;
    ducknng_mutex_unlock(&rt->mu);
    return 0;
}

ducknng_service *ducknng_runtime_remove_service(ducknng_runtime *rt, const char *name) {
    size_t i;
    ducknng_service *svc = NULL;
    if (!rt || !name) return NULL;
    ducknng_mutex_lock(&rt->mu);
    for (i = 0; i < rt->service_count; i++) {
        if (rt->services[i] && strcmp(rt->services[i]->name, name) == 0) {
            svc = rt->services[i];
            for (; i + 1 < rt->service_count; i++) rt->services[i] = rt->services[i + 1];
            rt->service_count--;
            break;
        }
    }
    ducknng_mutex_unlock(&rt->mu);
    return svc;
}
