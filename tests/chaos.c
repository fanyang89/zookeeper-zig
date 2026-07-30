// Chaos test: continuously perform sync API operations while an external
// script injects faults (docker pause/kill/restart).  The client is
// expected to survive transient errors (ZCONNECTIONLOSS,
// ZOPERATIONTIMEOUT, ZSESSIONEXPIRED) and eventually resume.
//
// This exercises code paths that the happy-path e2e suite never touches:
// reconnection logic, session-expiry handling, and the sync API itself.

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <zookeeper.h>

#define ITERATIONS 500
#define OPS_INTERVAL_US 100000 /* 100ms between iterations */
#define RECOVERY_WAIT_US 1000000 /* 1s back-off after transient error */

static atomic_int g_connected = 0;
static atomic_int g_expired = 0;

static void global_watcher(zhandle_t *zh, int type, int state, const char *path,
                           void *ctx) {
    (void)zh;
    (void)path;
    (void)ctx;
    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE)
            atomic_store(&g_connected, 1);
        else if (state == ZOO_EXPIRED_SESSION_STATE)
            atomic_store(&g_expired, 1);
        else
            atomic_store(&g_connected, 0);
    }
}

static int wait_connected(zhandle_t *zh, int timeout_s) {
    int64_t deadline = (int64_t)time(NULL) + timeout_s;
    while ((int64_t)time(NULL) < deadline) {
        if (atomic_load(&g_connected))
            return 0;
        usleep(100000);
    }
    return 1;
}

int main(int argc, char **argv) {
    const char *host = argc > 1 ? argv[1] : "127.0.0.1:2181";
    zhandle_t *zh = NULL;
    int ok = 0, transient = 0, fatal = 0;
    int i;

    zoo_set_debug_level(ZOO_LOG_LEVEL_ERROR);

    zh = zookeeper_init(host, global_watcher, 3000, NULL, NULL, 0);
    if (!zh) {
        fprintf(stderr, "FATAL: zookeeper_init failed\n");
        return 1;
    }

    if (wait_connected(zh, 15) != 0) {
        fprintf(stderr, "FATAL: no initial connection within 15s\n");
        zookeeper_close(zh);
        return 1;
    }
    printf("connected to %s\n", host);

    for (i = 0; i < ITERATIONS; i++) {
        char path[128];
        char buf[256];
        int buflen;
        struct Stat stat;

        snprintf(path, sizeof(path), "/chaos-%04d", i);

        /* --- create --- */
        int rc = zoo_create(zh, path, "hello", 5, &ZOO_OPEN_ACL_UNSAFE,
                            ZOO_EPHEMERAL, NULL, 0);
        if (rc != ZOK) {
            if (rc == ZCONNECTIONLOSS || rc == ZOPERATIONTIMEOUT ||
                rc == ZSESSIONEXPIRED || rc == ZINVALIDSTATE) {
                transient++;
                fprintf(stderr, "[%03d] create transient: %s\n", i, zerror(rc));
                /* if session expired, re-init handle */
                if (rc == ZSESSIONEXPIRED || rc == ZINVALIDSTATE) {
                    zookeeper_close(zh);
                    atomic_store(&g_connected, 0);
                    atomic_store(&g_expired, 0);
    zh = zookeeper_init(host, global_watcher, 10000, NULL, NULL, 0);
                    if (!zh) {
                        fprintf(stderr, "[%03d] re-init failed\n", i);
                        break;
                    }
                    wait_connected(zh, 15);
                }
                usleep(RECOVERY_WAIT_US);
            } else {
                fatal++;
                fprintf(stderr, "[%03d] create unexpected: %s\n", i, zerror(rc));
            }
            continue;
        }

        /* --- get --- */
        buflen = sizeof(buf);
        rc = zoo_get(zh, path, 0, buf, &buflen, &stat);
        if (rc == ZOK && buflen == 5 && memcmp(buf, "hello", 5) == 0 &&
            stat.version == 0) {
            ok++;
        } else if (rc != ZOK) {
            transient++;
            fprintf(stderr, "[%03d] get error: %s\n", i, zerror(rc));
        } else {
            fatal++;
            fprintf(stderr, "[%03d] get data mismatch\n", i);
        }

        /* --- set --- */
        rc = zoo_set(zh, path, "world", 5, 0);
        if (rc != ZOK && rc != ZCONNECTIONLOSS && rc != ZOPERATIONTIMEOUT) {
            fprintf(stderr, "[%03d] set error: %s\n", i, zerror(rc));
        }

        /* --- exists --- */
        rc = zoo_exists(zh, path, 0, &stat);
        if (rc == ZOK && stat.version == 1) {
            /* good */
        }

        /* --- delete --- */
        rc = zoo_delete(zh, path, 1);
        if (rc != ZOK && rc != ZCONNECTIONLOSS && rc != ZOPERATIONTIMEOUT &&
            rc != ZNONODE) {
            fprintf(stderr, "[%03d] delete error: %s\n", i, zerror(rc));
        }

        usleep(OPS_INTERVAL_US);
    }

    printf("\n=== Chaos Results ===\n");
    printf("iterations: %d\n", ITERATIONS);
    printf("ok:          %d\n", ok);
    printf("transient:   %d\n", transient);
    printf("fatal:       %d\n", fatal);
    printf("final state: %s%s\n",
           atomic_load(&g_connected) ? "connected" : "disconnected",
           atomic_load(&g_expired) ? " (session expired at least once)" : "");

    if (zh)
        zookeeper_close(zh);

    /* success: completed all iterations with some operations succeeding */
    return (ok > 0 && fatal < ITERATIONS / 2) ? 0 : 1;
}
