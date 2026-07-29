// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/select.h>
#include <time.h>
#include <unistd.h>

#include <zookeeper.h>

#define TIMEOUT_MS 10000
#define CHECK(condition)                                                      \
    do {                                                                      \
        if (!(condition)) {                                                   \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, \
                    #condition);                                              \
            goto cleanup;                                                     \
        }                                                                     \
    } while (0)

struct result {
    atomic_int done;
    int rc;
    char value[128];
    int value_len;
    struct Stat stat;
};

static int64_t monotonic_ms(void) {
    struct timespec ts;

    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1;
    }
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int drive_once(zhandle_t *zh) {
#ifdef THREADED
    (void)zh;
    usleep(10000);
    return ZOK;
#else
    int fd;
    int interest;
    int events = 0;
    int rc;
    fd_set read_fds;
    fd_set write_fds;
    fd_set error_fds;
    struct timeval timeout;

    rc = zookeeper_interest(zh, &fd, &interest, &timeout);
    if (rc != ZOK) {
        return rc;
    }
    if (timeout.tv_sec > 0 || timeout.tv_usec > 100000) {
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000;
    }

    FD_ZERO(&read_fds);
    FD_ZERO(&write_fds);
    FD_ZERO(&error_fds);
    if (fd >= 0) {
        if (interest & ZOOKEEPER_READ) {
            FD_SET(fd, &read_fds);
        }
        if (interest & ZOOKEEPER_WRITE) {
            FD_SET(fd, &write_fds);
        }
        FD_SET(fd, &error_fds);
    }

    rc = select(fd + 1, &read_fds, &write_fds, &error_fds, &timeout);
    if (rc < 0) {
        return errno == EINTR ? ZOK : ZSYSTEMERROR;
    }
    if (fd >= 0 && rc > 0) {
        if (FD_ISSET(fd, &read_fds)) {
            events |= ZOOKEEPER_READ;
        }
        if (FD_ISSET(fd, &write_fds)) {
            events |= ZOOKEEPER_WRITE;
        }
    }
    return zookeeper_process(zh, events);
#endif
}

static int wait_for_connection(zhandle_t *zh) {
    int64_t deadline = monotonic_ms() + TIMEOUT_MS;

    while (zoo_state(zh) != ZOO_CONNECTED_STATE) {
        int state = zoo_state(zh);
        int rc;

        if (state == ZOO_EXPIRED_SESSION_STATE ||
            state == ZOO_AUTH_FAILED_STATE || monotonic_ms() >= deadline) {
            return 1;
        }
        rc = drive_once(zh);
        if (rc != ZOK && rc != ZNOTHING) {
            return 1;
        }
    }
    return 0;
}

static int wait_for_result(zhandle_t *zh, struct result *result) {
    int64_t deadline = monotonic_ms() + TIMEOUT_MS;

    while (!atomic_load_explicit(&result->done, memory_order_acquire)) {
        int rc;

        if (monotonic_ms() >= deadline) {
            return 1;
        }
        rc = drive_once(zh);
        if (rc != ZOK && rc != ZNOTHING) {
            return 1;
        }
    }
    return 0;
}

static void string_completion(int rc, const char *value, const void *data) {
    struct result *result = (struct result *)data;

    result->rc = rc;
    if (rc == ZOK && value != NULL) {
        snprintf(result->value, sizeof(result->value), "%s", value);
    }
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void data_completion(int rc, const char *value, int value_len,
                            const struct Stat *stat, const void *data) {
    struct result *result = (struct result *)data;

    result->rc = rc;
    if (rc == ZOK) {
        result->value_len = value_len;
        if (value_len >= 0 && value_len <= (int)sizeof(result->value)) {
            memcpy(result->value, value, value_len);
        }
        if (stat != NULL) {
            result->stat = *stat;
        }
    }
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void stat_completion(int rc, const struct Stat *stat, const void *data) {
    struct result *result = (struct result *)data;

    result->rc = rc;
    if (rc == ZOK && stat != NULL) {
        result->stat = *stat;
    }
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void void_completion(int rc, const void *data) {
    struct result *result = (struct result *)data;

    result->rc = rc;
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void init_result(struct result *result) {
    memset(result, 0, sizeof(*result));
    atomic_init(&result->done, 0);
    result->rc = ZSYSTEMERROR;
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1:2181";
    const char initial[] = {'i', 'n', 'i', 't', 'i', 'a', 'l', '\0', 'x'};
    const char updated[] = "updated";
    char path[128];
    char child[160];
    zhandle_t *zh = NULL;
    struct result result;
    int status = 1;

    snprintf(path, sizeof(path), "/zookeeper-zig-e2e-%ld", (long)getpid());
    snprintf(child, sizeof(child), "%s/ephemeral", path);
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);
#ifdef HAVE_OPENSSL_H
    if (argc > 2) {
        zh = zookeeper_init_ssl(host, argv[2], NULL, 5000, NULL, NULL, 0);
    } else {
        zh = zookeeper_init(host, NULL, 5000, NULL, NULL, 0);
    }
#else
    if (argc > 2) {
        fprintf(stderr, "TLS credentials require -Dtls=true\n");
        return 1;
    }
    zh = zookeeper_init(host, NULL, 5000, NULL, NULL, 0);
#endif
    CHECK(zh != NULL);
    CHECK(wait_for_connection(zh) == 0);

    init_result(&result);
    CHECK(zoo_acreate(zh, path, initial, sizeof(initial), &ZOO_OPEN_ACL_UNSAFE,
                      0, string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZOK);
    CHECK(strcmp(result.value, path) == 0);

    init_result(&result);
    CHECK(zoo_aget(zh, path, 0, data_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZOK);
    CHECK(result.value_len == (int)sizeof(initial));
    CHECK(memcmp(result.value, initial, sizeof(initial)) == 0);
    CHECK(result.stat.version == 0);

    init_result(&result);
    CHECK(zoo_aset(zh, path, updated, sizeof(updated) - 1, 0, stat_completion,
                   &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZOK);
    CHECK(result.stat.version == 1);

    init_result(&result);
    CHECK(zoo_aget(zh, path, 0, data_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZOK);
    CHECK(result.value_len == (int)sizeof(updated) - 1);
    CHECK(memcmp(result.value, updated, sizeof(updated) - 1) == 0);
    CHECK(result.stat.version == 1);

    init_result(&result);
    CHECK(zoo_acreate(zh, child, NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
                      ZOO_EPHEMERAL, string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZOK);
    CHECK(strcmp(result.value, child) == 0);

    init_result(&result);
    CHECK(zoo_adelete(zh, child, -1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZOK);

    init_result(&result);
    CHECK(zoo_adelete(zh, path, 1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZOK);

    init_result(&result);
    CHECK(zoo_aexists(zh, path, 0, stat_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result) == 0);
    CHECK(result.rc == ZNONODE);

    status = 0;
    printf("PASS: %s client CRUD against %s%s\n",
#ifdef THREADED
           "threaded",
#else
           "single-threaded",
#endif
           host, argc > 2 ? " with TLS" : "");

cleanup:
    if (zh != NULL && zookeeper_close(zh) != ZOK) {
        status = 1;
    }
    return status;
}
