// libFuzzer harness for ZooKeeper jute deserialization.
//
// The fuzzer feeds arbitrary bytes into every server-to-client deserializer.
// A single selector byte at the front of the input picks which message type to
// exercise, giving the fuzzer a stable dispatch boundary while still exploring
// the full structure of each record.

#include <limits.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <recordio.h>
#include <zookeeper.jute.h>

int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    if (size < 1 || size - 1 > INT_MAX)
        return 0;

    const uint8_t selector = data[0];
    data++;
    size--;

    char *buf = malloc(size);
    if (!buf)
        return 0;
    memcpy(buf, data, size);

    struct iarchive *ia = create_buffer_iarchive(buf, (int)size);
    if (!ia) {
        free(buf);
        return 0;
    }

    switch (selector % 12) {
    case 0: {
        struct ConnectResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_ConnectResponse(ia, "fuzz", &r);
        deallocate_ConnectResponse(&r);
        break;
    }
    case 1: {
        struct ReplyHeader r;
        memset(&r, 0, sizeof(r));
        deserialize_ReplyHeader(ia, "fuzz", &r);
        deallocate_ReplyHeader(&r);
        break;
    }
    case 2: {
        struct GetDataResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_GetDataResponse(ia, "fuzz", &r);
        deallocate_GetDataResponse(&r);
        break;
    }
    case 3: {
        struct ExistsResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_ExistsResponse(ia, "fuzz", &r);
        deallocate_ExistsResponse(&r);
        break;
    }
    case 4: {
        struct GetACLResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_GetACLResponse(ia, "fuzz", &r);
        deallocate_GetACLResponse(&r);
        break;
    }
    case 5: {
        struct GetChildren2Response r;
        memset(&r, 0, sizeof(r));
        deserialize_GetChildren2Response(ia, "fuzz", &r);
        deallocate_GetChildren2Response(&r);
        break;
    }
    case 6: {
        struct Create2Response r;
        memset(&r, 0, sizeof(r));
        deserialize_Create2Response(ia, "fuzz", &r);
        deallocate_Create2Response(&r);
        break;
    }
    case 7: {
        struct ErrorResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_ErrorResponse(ia, "fuzz", &r);
        deallocate_ErrorResponse(&r);
        break;
    }
    case 8: {
        struct SetDataResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_SetDataResponse(ia, "fuzz", &r);
        deallocate_SetDataResponse(&r);
        break;
    }
    case 9: {
        struct SetACLResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_SetACLResponse(ia, "fuzz", &r);
        deallocate_SetACLResponse(&r);
        break;
    }
    case 10: {
        struct GetChildrenResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_GetChildrenResponse(ia, "fuzz", &r);
        deallocate_GetChildrenResponse(&r);
        break;
    }
    case 11: {
        struct SetSASLResponse r;
        memset(&r, 0, sizeof(r));
        deserialize_SetSASLResponse(ia, "fuzz", &r);
        deallocate_SetSASLResponse(&r);
        break;
    }
    }

    close_buffer_iarchive(&ia);
    free(buf);
    return 0;
}
