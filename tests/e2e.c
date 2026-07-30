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

/* ------------------------------------------------------------------ */
/* result / event structures                                           */
/* ------------------------------------------------------------------ */

struct result {
    atomic_int done;
    int rc;
    char value[128];
    int value_len;
    struct Stat stat;
};

struct children_result {
    atomic_int done;
    int rc;
    int32_t count;
    struct Stat stat;
};

struct acl_result {
    atomic_int done;
    int rc;
    int32_t count;
    int32_t perms;
    char scheme[32];
    char id[32];
    struct Stat stat;
};

struct watch_event {
    atomic_int fired;
    int type;
    int state;
    char path[128];
};

/* ------------------------------------------------------------------ */
/* completion callbacks                                               */
/* ------------------------------------------------------------------ */

static void string_completion(int rc, const char *value, const void *data) {
    struct result *result = (struct result *)data;
    result->rc = rc;
    if (rc == ZOK && value != NULL)
        snprintf(result->value, sizeof(result->value), "%s", value);
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void data_completion(int rc, const char *value, int value_len,
                            const struct Stat *stat, const void *data) {
    struct result *result = (struct result *)data;
    result->rc = rc;
    if (rc == ZOK) {
        result->value_len = value_len;
        if (value_len >= 0 && value_len <= (int)sizeof(result->value))
            memcpy(result->value, value, value_len);
        if (stat != NULL)
            result->stat = *stat;
    }
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void stat_completion(int rc, const struct Stat *stat, const void *data) {
    struct result *result = (struct result *)data;
    result->rc = rc;
    if (rc == ZOK && stat != NULL)
        result->stat = *stat;
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void void_completion(int rc, const void *data) {
    struct result *result = (struct result *)data;
    result->rc = rc;
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void string_stat_completion(int rc, const char *value,
                                   const struct Stat *stat, const void *data) {
    struct result *result = (struct result *)data;
    result->rc = rc;
    if (rc == ZOK) {
        if (value != NULL)
            snprintf(result->value, sizeof(result->value), "%s", value);
        if (stat != NULL)
            result->stat = *stat;
    }
    atomic_store_explicit(&result->done, 1, memory_order_release);
}

static void strings_completion(int rc, const struct String_vector *strings,
                               const void *data) {
    struct children_result *r = (struct children_result *)data;
    r->rc = rc;
    if (rc == ZOK)
        r->count = strings ? strings->count : 0;
    atomic_store_explicit(&r->done, 1, memory_order_release);
}

static void strings_stat_completion(int rc, const struct String_vector *strings,
                                    const struct Stat *stat, const void *data) {
    struct children_result *r = (struct children_result *)data;
    r->rc = rc;
    if (rc == ZOK) {
        /* strings vector is freed after callback returns — copy count now */
        r->count = strings ? strings->count : 0;
        if (stat != NULL)
            r->stat = *stat;
    }
    atomic_store_explicit(&r->done, 1, memory_order_release);
}

static void acl_completion(int rc, struct ACL_vector *acl, struct Stat *stat,
                           const void *data) {
    struct acl_result *r = (struct acl_result *)data;
    r->rc = rc;
    if (rc == ZOK && acl != NULL) {
        /* ACL_vector is freed after callback returns — copy fields now */
        r->count = acl->count;
        if (acl->count > 0 && acl->data != NULL) {
            r->perms = acl->data[0].perms;
            snprintf(r->scheme, sizeof(r->scheme), "%s",
                     acl->data[0].id.scheme);
            snprintf(r->id, sizeof(r->id), "%s", acl->data[0].id.id);
        }
        if (stat != NULL)
            r->stat = *stat;
    }
    atomic_store_explicit(&r->done, 1, memory_order_release);
}

static void watcher(zhandle_t *zh, int type, int state, const char *path,
                    void *ctx) {
    struct watch_event *ev = (struct watch_event *)ctx;
    (void)zh;
    ev->type = type;
    ev->state = state;
    if (path != NULL)
        snprintf(ev->path, sizeof(ev->path), "%s", path);
    atomic_store_explicit(&ev->fired, 1, memory_order_release);
}

/* ------------------------------------------------------------------ */
/* helpers                                                            */
/* ------------------------------------------------------------------ */

static int64_t monotonic_ms(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0)
        return -1;
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
    fd_set read_fds, write_fds, error_fds;
    struct timeval timeout;

    rc = zookeeper_interest(zh, &fd, &interest, &timeout);
    if (rc != ZOK)
        return rc;
    if (timeout.tv_sec > 0 || timeout.tv_usec > 100000) {
        timeout.tv_sec = 0;
        timeout.tv_usec = 100000;
    }
    FD_ZERO(&read_fds);
    FD_ZERO(&write_fds);
    FD_ZERO(&error_fds);
    if (fd >= 0) {
        if (interest & ZOOKEEPER_READ)  FD_SET(fd, &read_fds);
        if (interest & ZOOKEEPER_WRITE) FD_SET(fd, &write_fds);
        FD_SET(fd, &error_fds);
    }
    rc = select(fd + 1, &read_fds, &write_fds, &error_fds, &timeout);
    if (rc < 0)
        return errno == EINTR ? ZOK : ZSYSTEMERROR;
    if (fd >= 0 && rc > 0) {
        if (FD_ISSET(fd, &read_fds))  events |= ZOOKEEPER_READ;
        if (FD_ISSET(fd, &write_fds)) events |= ZOOKEEPER_WRITE;
    }
    return zookeeper_process(zh, events);
#endif
}

static int wait_for_connection(zhandle_t *zh) {
    int64_t deadline = monotonic_ms() + TIMEOUT_MS;
    while (zoo_state(zh) != ZOO_CONNECTED_STATE) {
        int state = zoo_state(zh);
        if (state == ZOO_EXPIRED_SESSION_STATE ||
            state == ZOO_AUTH_FAILED_STATE ||
            monotonic_ms() >= deadline)
            return 1;
        if (drive_once(zh) != ZOK && drive_once(zh) != ZNOTHING)
            return 1;
    }
    return 0;
}

static int wait_for_result(zhandle_t *zh, atomic_int *done) {
    int64_t deadline = monotonic_ms() + TIMEOUT_MS;
    while (!atomic_load_explicit(done, memory_order_acquire)) {
        int rc;
        if (monotonic_ms() >= deadline)
            return 1;
        rc = drive_once(zh);
        if (rc != ZOK && rc != ZNOTHING)
            return 1;
    }
    return 0;
}

static void init_result(struct result *r) {
    memset(r, 0, sizeof(*r));
    atomic_init(&r->done, 0);
    r->rc = ZSYSTEMERROR;
}

static void init_watch_event(struct watch_event *ev) {
    memset(ev, 0, sizeof(*ev));
    atomic_init(&ev->fired, 0);
}

static const char *mode_label(void) {
    return
#ifdef THREADED
        "threaded"
#else
        "single-threaded"
#endif
    ;
}

/* ------------------------------------------------------------------ */
/* test: CRUD                                                         */
/* ------------------------------------------------------------------ */

static int test_crud(zhandle_t *zh, const char *prefix) {
    char path[128];
    char child[160];
    const char initial[] = {'i', 'n', 'i', 't', 'i', 'a', 'l', '\0', 'x'};
    const char updated[] = "updated";
    struct result result;
    int status = 1;

    snprintf(path, sizeof(path), "%s/crud", prefix);
    snprintf(child, sizeof(child), "%s/ephemeral", path);

    init_result(&result);
    CHECK(zoo_acreate(zh, path, initial, sizeof(initial), &ZOO_OPEN_ACL_UNSAFE,
                      0, string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);
    CHECK(strcmp(result.value, path) == 0);

    init_result(&result);
    CHECK(zoo_aget(zh, path, 0, data_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);
    CHECK(result.value_len == (int)sizeof(initial));
    CHECK(memcmp(result.value, initial, sizeof(initial)) == 0);
    CHECK(result.stat.version == 0);

    init_result(&result);
    CHECK(zoo_aset(zh, path, updated, sizeof(updated) - 1, 0,
                   stat_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);
    CHECK(result.stat.version == 1);

    init_result(&result);
    CHECK(zoo_aget(zh, path, 0, data_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.value_len == (int)sizeof(updated) - 1);
    CHECK(memcmp(result.value, updated, sizeof(updated) - 1) == 0);

    init_result(&result);
    CHECK(zoo_acreate(zh, child, NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
                      ZOO_EPHEMERAL, string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);
    CHECK(strcmp(result.value, child) == 0);

    init_result(&result);
    CHECK(zoo_adelete(zh, child, -1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    init_result(&result);
    CHECK(zoo_adelete(zh, path, 1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    init_result(&result);
    CHECK(zoo_aexists(zh, path, 0, stat_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZNONODE);

    status = 0;
    printf("PASS: %s CRUD\n", mode_label());
cleanup:
    return status;
}

/* ------------------------------------------------------------------ */
/* test: watches                                                      */
/* ------------------------------------------------------------------ */

static int test_watches(zhandle_t *zh, const char *prefix) {
    char path[128];
    struct result result;
    struct watch_event wev;
    int status = 1;

    /* --- data watch: awget → aset triggers ZOO_CHANGED_EVENT --- */
    snprintf(path, sizeof(path), "%s/watch-data", prefix);

    init_result(&result);
    CHECK(zoo_acreate(zh, path, "v0", 2, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    init_result(&result);
    init_watch_event(&wev);
    CHECK(zoo_awget(zh, path, watcher, &wev, data_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    /* watch is now registered on the server; modify data to trigger it */
    init_result(&result);
    CHECK(zoo_aset(zh, path, "v1", 2, 0, stat_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    CHECK(wait_for_result(zh, &wev.fired) == 0);
    CHECK(wev.type == ZOO_CHANGED_EVENT);

    init_result(&result);
    CHECK(zoo_adelete(zh, path, 1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);

    /* --- exist watch: awexists(missing) → acreate triggers
     * ZOO_CREATED_EVENT --- */
    snprintf(path, sizeof(path), "%s/watch-exists", prefix);

    init_result(&result);
    init_watch_event(&wev);
    /* awexists on a missing node registers a watch and returns ZNONODE */
    CHECK(zoo_awexists(zh, path, watcher, &wev, stat_completion,
                       &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZNONODE);

    init_result(&result);
    CHECK(zoo_acreate(zh, path, "created", 6, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    CHECK(wait_for_result(zh, &wev.fired) == 0);
    CHECK(wev.type == ZOO_CREATED_EVENT);

    init_result(&result);
    CHECK(zoo_adelete(zh, path, -1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);

    /* --- children watch: awget_children2 → acreate child triggers
     * ZOO_CHILD_EVENT --- */
    snprintf(path, sizeof(path), "%s/watch-children", prefix);

    init_result(&result);
    CHECK(zoo_acreate(zh, path, NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    {
        struct children_result cr;
        char child_path[160];
        memset(&cr, 0, sizeof(cr));
        atomic_init(&cr.done, 0);
        init_watch_event(&wev);
        CHECK(zoo_awget_children2(zh, path, watcher, &wev,
                                  strings_stat_completion, &cr) == ZOK);
        CHECK(wait_for_result(zh, &cr.done) == 0);
        CHECK(cr.rc == ZOK);
        CHECK(cr.count == 0);

        snprintf(child_path, sizeof(child_path), "%s/c1", path);
        init_result(&result);
        CHECK(zoo_acreate(zh, child_path, NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
                          ZOO_EPHEMERAL, string_completion, &result) == ZOK);
        CHECK(wait_for_result(zh, &result.done) == 0);
        CHECK(result.rc == ZOK);

        CHECK(wait_for_result(zh, &wev.fired) == 0);
        CHECK(wev.type == ZOO_CHILD_EVENT);

        init_result(&result);
        CHECK(zoo_adelete(zh, child_path, -1, void_completion,
                          &result) == ZOK);
        CHECK(wait_for_result(zh, &result.done) == 0);
    }

    init_result(&result);
    CHECK(zoo_adelete(zh, path, -1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);

    status = 0;
    printf("PASS: %s watches\n", mode_label());
cleanup:
    return status;
}

/* ------------------------------------------------------------------ */
/* test: children                                                     */
/* ------------------------------------------------------------------ */

static int test_children(zhandle_t *zh, const char *prefix) {
    char parent[128];
    char child[160];
    struct result result;
    struct children_result cr;
    int status = 1;
    int i;

    snprintf(parent, sizeof(parent), "%s/children", prefix);

    init_result(&result);
    CHECK(zoo_acreate(zh, parent, NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    for (i = 0; i < 3; i++) {
        snprintf(child, sizeof(child), "%s/c%d", parent, i);
        init_result(&result);
        CHECK(zoo_acreate(zh, child, NULL, -1, &ZOO_OPEN_ACL_UNSAFE,
                          ZOO_EPHEMERAL, string_completion, &result) == ZOK);
        CHECK(wait_for_result(zh, &result.done) == 0);
        CHECK(result.rc == ZOK);
    }

    memset(&cr, 0, sizeof(cr));
    atomic_init(&cr.done, 0);
    cr.rc = ZSYSTEMERROR;
    CHECK(zoo_aget_children2(zh, parent, 0, strings_stat_completion,
                             &cr) == ZOK);
    CHECK(wait_for_result(zh, &cr.done) == 0);
    CHECK(cr.rc == ZOK);
    CHECK(cr.count == 3);

    /* also test zoo_aget_children (no Stat variant) */
    {
        struct children_result cr2;
        memset(&cr2, 0, sizeof(cr2));
        atomic_init(&cr2.done, 0);
        cr2.rc = ZSYSTEMERROR;
        CHECK(zoo_aget_children(zh, parent, 0, strings_completion,
                                &cr2) == ZOK);
        CHECK(wait_for_result(zh, &cr2.done) == 0);
        CHECK(cr2.rc == ZOK);
        CHECK(cr2.count == 3);
    }

    init_result(&result);
    CHECK(zoo_adelete(zh, parent, -1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    /* ephemeral children are removed when the session ends, but the
     * parent is PERSISTENT so it may fail with ZNOTEMPTY if children
     * haven't been cleaned up yet — retry */
    if (result.rc != ZOK) {
        usleep(500000);
        init_result(&result);
        zoo_adelete(zh, parent, -1, void_completion, &result);
        wait_for_result(zh, &result.done);
    }

    status = 0;
    printf("PASS: %s children\n", mode_label());
cleanup:
    return status;
}

/* ------------------------------------------------------------------ */
/* test: multi                                                        */
/* ------------------------------------------------------------------ */

static int test_multi(zhandle_t *zh, const char *prefix) {
    char path1[128], path2[128];
    struct result result;
    int status = 1;

    snprintf(path1, sizeof(path1), "%s/multi-1", prefix);
    snprintf(path2, sizeof(path2), "%s/multi-2", prefix);

    init_result(&result);
    CHECK(zoo_acreate(zh, path1, "init1", 5, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    init_result(&result);
    CHECK(zoo_acreate(zh, path2, "init2", 5, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    /* success: set both nodes atomically */
    {
        zoo_op_t ops[2];
        zoo_op_result_t results[2];
        struct Stat stat1, stat2;

        zoo_set_op_init(&ops[0], path1, "hello", 5, 0, &stat1);
        zoo_set_op_init(&ops[1], path2, "world", 5, 0, &stat2);

        init_result(&result);
        CHECK(zoo_amulti(zh, 2, ops, results, void_completion,
                         &result) == ZOK);
        CHECK(wait_for_result(zh, &result.done) == 0);
        CHECK(result.rc == ZOK);
        CHECK(results[0].err == ZOK);
        CHECK(results[1].err == ZOK);
    }

    /* failure + rollback: set node1 ok, delete node2 wrong-version → all abort */
    {
        zoo_op_t ops[2];
        zoo_op_result_t results[2];

        zoo_set_op_init(&ops[0], path1, "rolled", 6, 1, NULL);
        zoo_delete_op_init(&ops[1], path2, 999);

        init_result(&result);
        CHECK(zoo_amulti(zh, 2, ops, results, void_completion,
                         &result) == ZOK);
        CHECK(wait_for_result(zh, &result.done) == 0);
        /* multi aborts: result.rc reflects the failing op */
        CHECK(result.rc != ZOK);
        /* node1 data must be unchanged ("hello", not "rolled") */
        init_result(&result);
        CHECK(zoo_aget(zh, path1, 0, data_completion, &result) == ZOK);
        CHECK(wait_for_result(zh, &result.done) == 0);
        CHECK(result.rc == ZOK);
        CHECK(result.value_len == 5);
        CHECK(memcmp(result.value, "hello", 5) == 0);
    }

    /* success: create + check + set in one transaction */
    {
        char create_path[128];
        zoo_op_t ops[3];
        zoo_op_result_t results[3];
        struct Stat check_stat, set_stat;
        char created_path[160];

        snprintf(create_path, sizeof(create_path), "%s/multi-created", prefix);
        zoo_create_op_init(&ops[0], create_path, "new", 3,
                           &ZOO_OPEN_ACL_UNSAFE, 0,
                           created_path, sizeof(created_path));
        zoo_check_op_init(&ops[1], create_path, 0);
        zoo_set_op_init(&ops[2], create_path, "upd", 3, 0, &set_stat);

        init_result(&result);
        CHECK(zoo_amulti(zh, 3, ops, results, void_completion,
                         &result) == ZOK);
        CHECK(wait_for_result(zh, &result.done) == 0);
        CHECK(result.rc == ZOK);
        CHECK(results[0].err == ZOK);
        CHECK(results[1].err == ZOK);
        CHECK(results[2].err == ZOK);

        /* cleanup created node */
        init_result(&result);
        zoo_adelete(zh, create_path, 1, void_completion, &result);
        wait_for_result(zh, &result.done);
    }

    /* cleanup */
    init_result(&result);
    zoo_adelete(zh, path1, -1, void_completion, &result);
    wait_for_result(zh, &result.done);
    init_result(&result);
    zoo_adelete(zh, path2, -1, void_completion, &result);
    wait_for_result(zh, &result.done);

    status = 0;
    printf("PASS: %s multi\n", mode_label());
cleanup:
    return status;
}

/* ------------------------------------------------------------------ */
/* test: ACL                                                          */
/* ------------------------------------------------------------------ */

static int test_acl(zhandle_t *zh, const char *prefix) {
    char path[128];
    struct result result;
    struct acl_result ar;
    int status = 1;

    snprintf(path, sizeof(path), "%s/acl", prefix);

    init_result(&result);
    CHECK(zoo_acreate(zh, path, NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    /* default ACL should be OPEN_ACL_UNSAFE (perms=ALL, world:anyone) */
    memset(&ar, 0, sizeof(ar));
    atomic_init(&ar.done, 0);
    ar.rc = ZSYSTEMERROR;
    CHECK(zoo_aget_acl(zh, path, acl_completion, &ar) == ZOK);
    CHECK(wait_for_result(zh, &ar.done) == 0);
    CHECK(ar.rc == ZOK);
    CHECK(ar.count == 1);
    CHECK(ar.perms == ZOO_PERM_ALL);
    CHECK(strcmp(ar.scheme, "world") == 0);
    CHECK(strcmp(ar.id, "anyone") == 0);

    /* change to READ_ACL_UNSAFE (perms=READ only) */
    init_result(&result);
    CHECK(zoo_aset_acl(zh, path, -1, &ZOO_READ_ACL_UNSAFE, void_completion,
                       &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    /* verify */
    memset(&ar, 0, sizeof(ar));
    atomic_init(&ar.done, 0);
    ar.rc = ZSYSTEMERROR;
    CHECK(zoo_aget_acl(zh, path, acl_completion, &ar) == ZOK);
    CHECK(wait_for_result(zh, &ar.done) == 0);
    CHECK(ar.rc == ZOK);
    CHECK(ar.count == 1);
    CHECK(ar.perms == ZOO_PERM_READ);

    init_result(&result);
    CHECK(zoo_adelete(zh, path, -1, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);

    status = 0;
    printf("PASS: %s ACL\n", mode_label());
cleanup:
    return status;
}

/* ------------------------------------------------------------------ */
/* test: error paths                                                  */
/* ------------------------------------------------------------------ */

static int test_error_paths(zhandle_t *zh, const char *prefix) {
    char path[128];
    struct result result;
    int status = 1;

    snprintf(path, sizeof(path), "%s/errors", prefix);

    init_result(&result);
    CHECK(zoo_acreate(zh, path, "data", 4, &ZOO_OPEN_ACL_UNSAFE, 0,
                      string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    /* aset with wrong version → ZBADVERSION */
    init_result(&result);
    CHECK(zoo_aset(zh, path, "x", 1, 999, stat_completion,
                   &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZBADVERSION);

    /* aget on non-existent node → ZNONODE */
    init_result(&result);
    CHECK(zoo_aget(zh, "/zz-nonexistent-12345", 0, data_completion,
                   &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZNONODE);

    /* adelete with wrong version → ZBADVERSION */
    init_result(&result);
    CHECK(zoo_adelete(zh, path, 999, void_completion,
                      &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZBADVERSION);

    init_result(&result);
    CHECK(zoo_adelete(zh, path, 0, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    status = 0;
    printf("PASS: %s error paths\n", mode_label());
cleanup:
    return status;
}

/* ------------------------------------------------------------------ */
/* test: API helpers + sync + create2                                 */
/* ------------------------------------------------------------------ */

static int test_api(zhandle_t *zh, const char *prefix) {
    char path[128];
    struct result result;
    struct watch_event global_watch;
    int status = 1;

    /* simple getter/setter APIs */
    zoo_set_context(zh, (void *)0x42);
    CHECK(zoo_get_context(zh) == (void *)0x42);
    zoo_set_context(zh, NULL);

    CHECK(zoo_recv_timeout(zh) > 0);
    CHECK(zoo_client_id(zh) != NULL);

    /* global watcher */
    init_watch_event(&global_watch);
    zoo_set_watcher(zh, watcher);

    /* server info */
    {
        const char *srv = zoo_get_current_server(zh);
        CHECK(srv != NULL);
    }

    /* zerror */
    CHECK(zerror(ZOK) != NULL);
    CHECK(zerror(ZNONODE) != NULL);
    CHECK(zerror(ZBADVERSION) != NULL);

    /* zoo_async (flush to leader) */
    init_result(&result);
    CHECK(zoo_async(zh, "/", string_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    /* zoo_acreate2 — returns path + Stat */
    snprintf(path, sizeof(path), "%s/create2", prefix);
    init_result(&result);
    CHECK(zoo_acreate2(zh, path, "hello", 5, &ZOO_OPEN_ACL_UNSAFE, 0,
                       string_stat_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);
    CHECK(strcmp(result.value, path) == 0);
    CHECK(result.stat.version == 0);
    CHECK(result.stat.dataLength == 5);

    init_result(&result);
    CHECK(zoo_adelete(zh, path, 0, void_completion, &result) == ZOK);
    CHECK(wait_for_result(zh, &result.done) == 0);
    CHECK(result.rc == ZOK);

    status = 0;
    printf("PASS: %s API\n", mode_label());
cleanup:
    return status;
}

/* ------------------------------------------------------------------ */
/* main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1:2181";
    char prefix[128];
    zhandle_t *zh = NULL;
    int use_sasl = argc > 2 && strcmp(argv[2], "--sasl") == 0;
    int status = 1;
#ifdef HAVE_CYRUS_SASL_H
    sasl_callback_t *sasl_callbacks = NULL;
    int sasl_initialized = 0;
#endif

    snprintf(prefix, sizeof(prefix), "/zz-e2e-%ld", (long)getpid());
    zoo_set_debug_level(ZOO_LOG_LEVEL_WARN);

#ifdef HAVE_CYRUS_SASL_H
    if (use_sasl) {
        zoo_sasl_params_t params = {
            .service = "zookeeper",
            .host = "zk-sasl-md5",
            .mechlist = "DIGEST-MD5",
        };

        CHECK(argc > 3);
        CHECK(sasl_client_init(NULL) == SASL_OK);
        sasl_initialized = 1;
        sasl_callbacks = zoo_sasl_make_basic_callbacks("bob", NULL, argv[3]);
        CHECK(sasl_callbacks != NULL);
        params.callbacks = sasl_callbacks;
        zh = zookeeper_init_sasl(host, NULL, 5000, NULL, NULL, 0, NULL,
                                 &params);
    } else
#else
    if (use_sasl) {
        fprintf(stderr, "SASL credentials require -Dsasl=true\n");
        return 1;
    }
#endif
#ifdef HAVE_OPENSSL_H
    if (argc > 2 && !use_sasl) {
        zh = zookeeper_init_ssl(host, argv[2], NULL, 5000, NULL, NULL, 0);
    } else {
        zh = zookeeper_init(host, NULL, 5000, NULL, NULL, 0);
    }
#else
    if (argc > 2 && !use_sasl) {
        fprintf(stderr, "TLS credentials require -Dtls=true\n");
        return 1;
    }
    zh = zookeeper_init(host, NULL, 5000, NULL, NULL, 0);
#endif
    CHECK(zh != NULL);
    CHECK(wait_for_connection(zh) == 0);

    /* create the base prefix node so children like prefix/crud resolve */
    {
        struct result base_result;
        init_result(&base_result);
        if (zoo_acreate(zh, prefix, NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0,
                        string_completion, &base_result) != ZOK)
            goto cleanup;
        if (wait_for_result(zh, &base_result.done) != 0)
            goto cleanup;
        if (base_result.rc != ZOK)
            goto cleanup;
    }

    status = 0;
    status |= test_crud(zh, prefix);
    status |= test_watches(zh, prefix);
    status |= test_children(zh, prefix);
    status |= test_multi(zh, prefix);
    status |= test_acl(zh, prefix);
    status |= test_error_paths(zh, prefix);
    status |= test_api(zh, prefix);

    /* cleanup base node */
    {
        struct result base_result;
        init_result(&base_result);
        zoo_adelete(zh, prefix, -1, void_completion, &base_result);
        wait_for_result(zh, &base_result.done);
    }

    if (status == 0) {
        printf("PASS: %s client full suite against %s%s\n", mode_label(), host,
               use_sasl ? " with SASL"
                        : (argc > 2 && !use_sasl) ? " with TLS" : "");
    }

cleanup:
    if (zh != NULL && zookeeper_close(zh) != ZOK)
        status = 1;
#ifdef HAVE_CYRUS_SASL_H
    free(sasl_callbacks);
    if (sasl_initialized)
        sasl_done();
#endif
    return status;
}
