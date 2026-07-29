// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with this
// work for additional information regarding copyright ownership. The ASF
// licenses this file to you under the Apache License, Version 2.0.

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <zookeeper.h>

#define CHECK(condition)                                                      \
    do {                                                                      \
        if (!(condition)) {                                                   \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, \
                    #condition);                                              \
            return 1;                                                         \
        }                                                                     \
    } while (0)

static int test_error_strings(void) {
    CHECK(strcmp(ZOO_VERSION, "3.9.5") == 0);
    CHECK(strcmp(zerror(ZOK), "ok") == 0);
    CHECK(strcmp(zerror(ZCONNECTIONLOSS), "connection loss") == 0);
    CHECK(strcmp(zerror(ZNONODE), "no node") == 0);
    CHECK(strcmp(zerror(-9999), "unknown error") == 0);
    CHECK(strcmp(zerror(EINVAL), strerror(EINVAL)) == 0);
    return 0;
}

static int test_byte_order(void) {
    const int64_t values[] = {
        0,
        1,
        -1,
        INT64_C(0x0102030405060708),
        INT64_MIN,
        INT64_MAX,
    };
    size_t i;

    for (i = 0; i < sizeof(values) / sizeof(values[0]); ++i) {
        CHECK(zoo_htonll(zoo_htonll(values[i])) == values[i]);
    }
    return 0;
}

static int test_request_header_round_trip(void) {
    struct RequestHeader input = {.xid = INT32_C(0x12345678), .type = -42};
    struct RequestHeader output = {0};
    struct oarchive *oa = create_buffer_oarchive();
    struct iarchive *ia;

    CHECK(oa != NULL);
    CHECK(serialize_RequestHeader(oa, "header", &input) == 0);
    CHECK(get_buffer_len(oa) == 8);

    ia = create_buffer_iarchive(get_buffer(oa), get_buffer_len(oa));
    CHECK(ia != NULL);
    CHECK(deserialize_RequestHeader(ia, "header", &output) == 0);
    CHECK(output.xid == input.xid);
    CHECK(output.type == input.type);

    close_buffer_iarchive(&ia);
    close_buffer_oarchive(&oa, 1);
    CHECK(ia == NULL);
    CHECK(oa == NULL);
    return 0;
}

static int test_set_data_round_trip(void) {
    char payload[] = {'a', '\0', 'b', (char)0xff};
    struct SetDataRequest input = {
        .path = "/unit/data",
        .data = {.len = (int32_t)sizeof(payload), .buff = payload},
        .version = 17,
    };
    struct SetDataRequest output = {0};
    struct oarchive *oa = create_buffer_oarchive();
    struct iarchive *ia;

    CHECK(oa != NULL);
    CHECK(serialize_SetDataRequest(oa, "request", &input) == 0);
    ia = create_buffer_iarchive(get_buffer(oa), get_buffer_len(oa));
    CHECK(ia != NULL);
    CHECK(deserialize_SetDataRequest(ia, "request", &output) == 0);
    CHECK(strcmp(output.path, input.path) == 0);
    CHECK(output.version == input.version);
    CHECK(output.data.len == input.data.len);
    CHECK(memcmp(output.data.buff, input.data.buff, sizeof(payload)) == 0);

    deallocate_SetDataRequest(&output);
    close_buffer_iarchive(&ia);
    close_buffer_oarchive(&oa, 1);
    return 0;
}

static int test_truncated_record(void) {
    const char complete[] = {0x12, 0x34, 0x56, 0x78, 0, 0, 0, 1};
    int len;

    for (len = 0; len < (int)sizeof(complete); ++len) {
        struct RequestHeader output = {0};
        struct iarchive *ia = create_buffer_iarchive((char *)complete, len);
        CHECK(ia != NULL);
        CHECK(deserialize_RequestHeader(ia, "header", &output) != 0);
        close_buffer_iarchive(&ia);
    }
    return 0;
}

static int test_vectors(void) {
    struct String_vector strings = {0};
    struct ACL_vector acls = {0};

    CHECK(allocate_String_vector(&strings, 3) == 0);
    CHECK(strings.count == 3);
    strings.data[0] = strdup("one");
    strings.data[1] = strdup("two");
    strings.data[2] = strdup("three");
    CHECK(strings.data[0] != NULL);
    CHECK(strings.data[1] != NULL);
    CHECK(strings.data[2] != NULL);
    CHECK(deallocate_String_vector(&strings) == 0);
    CHECK(strings.count == 3);
    CHECK(strings.data == NULL);

    CHECK(allocate_ACL_vector(&acls, 2) == 0);
    CHECK(acls.count == 2);
    acls.data[0].perms = ZOO_PERM_READ;
    acls.data[0].id.scheme = strdup("world");
    acls.data[0].id.id = strdup("anyone");
    acls.data[1].perms = ZOO_PERM_ALL;
    acls.data[1].id.scheme = strdup("auth");
    acls.data[1].id.id = strdup("");
    CHECK(acls.data[0].id.scheme != NULL && acls.data[0].id.id != NULL);
    CHECK(acls.data[1].id.scheme != NULL && acls.data[1].id.id != NULL);
    CHECK(deallocate_ACL_vector(&acls) == 0);
    CHECK(acls.count == 2);
    CHECK(acls.data == NULL);
    return 0;
}

static int test_multi_initializers(void) {
    zoo_op_t op;
    struct Stat stat = {0};
    char path_buffer[32];

    zoo_create_op_init(&op, "/create", "data", 4, &ZOO_OPEN_ACL_UNSAFE,
                       ZOO_EPHEMERAL, path_buffer, sizeof(path_buffer));
    CHECK(op.type == ZOO_CREATE_OP);
    CHECK(strcmp(op.create_op.path, "/create") == 0);
    CHECK(op.create_op.datalen == 4);
    CHECK(op.create_op.flags == ZOO_EPHEMERAL);
    CHECK(op.create_op.buf == path_buffer);

    zoo_delete_op_init(&op, "/delete", 3);
    CHECK(op.type == ZOO_DELETE_OP);
    CHECK(strcmp(op.delete_op.path, "/delete") == 0);
    CHECK(op.delete_op.version == 3);

    zoo_set_op_init(&op, "/set", "value", 5, 4, &stat);
    CHECK(op.type == ZOO_SETDATA_OP);
    CHECK(strcmp(op.set_op.path, "/set") == 0);
    CHECK(op.set_op.datalen == 5);
    CHECK(op.set_op.version == 4);
    CHECK(op.set_op.stat == &stat);

    zoo_check_op_init(&op, "/check", 5);
    CHECK(op.type == ZOO_CHECK_OP);
    CHECK(strcmp(op.check_op.path, "/check") == 0);
    CHECK(op.check_op.version == 5);
    return 0;
}

static int test_invalid_hosts(void) {
    const char *invalid[] = {NULL, "", " ", "  ", "host"};
    size_t i;

    for (i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        zhandle_t *zh;
        errno = 0;
        zh = zookeeper_init(invalid[i], NULL, 1000, NULL, NULL, 0);
        CHECK(zh == NULL);
        CHECK(errno != 0);
    }
    return 0;
}

static int test_path_validation_and_close(void) {
    const char control_path[] = {'/', 'a', 1, 'b', '\0'};
    const char *invalid[] = {
        "",
        "relative",
        "/trailing/",
        "//double",
        "/./child",
        "/../child",
        control_path,
    };
    zhandle_t *zh = zookeeper_init("127.0.0.1:1", NULL, 1000, NULL, NULL, 0);
    size_t i;

    CHECK(zh != NULL);
    for (i = 0; i < sizeof(invalid) / sizeof(invalid[0]); ++i) {
        CHECK(zoo_acreate(zh, invalid[i], NULL, -1, &ZOO_OPEN_ACL_UNSAFE, 0,
                          NULL, NULL) == ZBADARGUMENTS);
    }
    CHECK(zookeeper_close(zh) == ZOK);
    return 0;
}

struct test_case {
    const char *name;
    int (*run)(void);
};

int main(void) {
    const struct test_case tests[] = {
        {"error strings", test_error_strings},
        {"byte order", test_byte_order},
        {"request header round trip", test_request_header_round_trip},
        {"set data round trip", test_set_data_round_trip},
        {"truncated record", test_truncated_record},
        {"vectors", test_vectors},
        {"multi initializers", test_multi_initializers},
        {"invalid hosts", test_invalid_hosts},
        {"path validation and close", test_path_validation_and_close},
    };
    FILE *log_sink = fopen("/dev/null", "w");
    size_t i;

    CHECK(log_sink != NULL);
    zoo_set_log_stream(log_sink);
    for (i = 0; i < sizeof(tests) / sizeof(tests[0]); ++i) {
        if (tests[i].run() != 0) {
            fprintf(stderr, "FAIL: %s\n", tests[i].name);
            fclose(log_sink);
            return 1;
        }
        printf("PASS: %s\n", tests[i].name);
    }
    CHECK(fclose(log_sink) == 0);
    return 0;
}
