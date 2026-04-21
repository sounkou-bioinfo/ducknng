#include "ducknng_thread.h"
#include "ducknng_util.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef _WIN32
#include <unistd.h>
#include <pthread.h>
#endif

DUCKDB_EXTENSION_EXTERN

char *ducknng_strdup(const char *s) {
    size_t n;
    char *out;
    if (!s) return NULL;
    n = strlen(s);
    out = (char *)duckdb_malloc(n + 1);
    if (!out) return NULL;
    memcpy(out, s, n + 1);
    return out;
}

uint64_t ducknng_now_ms(void) {
#ifndef _WIN32
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return ((uint64_t)ts.tv_sec * 1000ULL) + (uint64_t)(ts.tv_nsec / 1000000ULL);
#else
    return 0;
#endif
}

void ducknng_sleep_ms(uint64_t ms) {
#ifndef _WIN32
    usleep((useconds_t)(ms * 1000ULL));
#else
    (void)ms;
#endif
}

uint16_t ducknng_le16_read(const uint8_t *p) { return (uint16_t)(p[0] | ((uint16_t)p[1] << 8)); }
uint32_t ducknng_le32_read(const uint8_t *p) { return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) | ((uint32_t)p[3] << 24); }
uint64_t ducknng_le64_read(const uint8_t *p) { return (uint64_t)ducknng_le32_read(p) | ((uint64_t)ducknng_le32_read(p + 4) << 32); }
void ducknng_le16_write(uint8_t *p, uint16_t v) { p[0] = (uint8_t)(v & 0xff); p[1] = (uint8_t)((v >> 8) & 0xff); }
void ducknng_le32_write(uint8_t *p, uint32_t v) { p[0] = (uint8_t)(v & 0xff); p[1] = (uint8_t)((v >> 8) & 0xff); p[2] = (uint8_t)((v >> 16) & 0xff); p[3] = (uint8_t)((v >> 24) & 0xff); }
void ducknng_le64_write(uint8_t *p, uint64_t v) { ducknng_le32_write(p, (uint32_t)(v & 0xffffffffu)); ducknng_le32_write(p + 4, (uint32_t)(v >> 32)); }

int ducknng_thread_create(ducknng_thread *thread, void *(*fn)(void *), void *arg) {
#ifndef _WIN32
    return pthread_create(thread, NULL, fn, arg);
#else
    (void)thread; (void)fn; (void)arg; return -1;
#endif
}
void ducknng_thread_join(ducknng_thread thread) {
#ifndef _WIN32
    pthread_join(thread, NULL);
#else
    (void)thread;
#endif
}
int ducknng_mutex_init(ducknng_mutex *mu) {
#ifndef _WIN32
    return pthread_mutex_init(mu, NULL);
#else
    (void)mu; return -1;
#endif
}
void ducknng_mutex_lock(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_lock(mu);
#else
    (void)mu;
#endif
}
void ducknng_mutex_unlock(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_unlock(mu);
#else
    (void)mu;
#endif
}
void ducknng_mutex_destroy(ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_mutex_destroy(mu);
#else
    (void)mu;
#endif
}
int ducknng_cond_init(ducknng_cond *cv) {
#ifndef _WIN32
    return pthread_cond_init(cv, NULL);
#else
    (void)cv; return -1;
#endif
}
void ducknng_cond_wait(ducknng_cond *cv, ducknng_mutex *mu) {
#ifndef _WIN32
    pthread_cond_wait(cv, mu);
#else
    (void)cv; (void)mu;
#endif
}
void ducknng_cond_signal(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_signal(cv);
#else
    (void)cv;
#endif
}
void ducknng_cond_destroy(ducknng_cond *cv) {
#ifndef _WIN32
    pthread_cond_destroy(cv);
#else
    (void)cv;
#endif
}
