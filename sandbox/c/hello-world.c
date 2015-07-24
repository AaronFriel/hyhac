  /* WARNING:  This code omits all error checking and is for illustration only */
  /* HyperDex Includes */
#include <string.h>
#include <stdio.h>
#include <hyperdex/client.h>
int
main(int argc, const char* argv[])
{
    struct hyperdex_client* client = NULL;

    struct hyperdex_client_attribute attr;
    const struct hyperdex_client_attribute* attrs;

    // struct hyperdex_client_map_attribute mapattr;

    size_t attrs_sz = 0;
    int64_t op_id;
    enum hyperdex_client_returncode op_status;

    int64_t loop_id;
    enum hyperdex_client_returncode loop_status;
    size_t i;

    client = hyperdex_client_create("127.0.0.1", 1982);
    
    // {
    //     struct hyperdex_client_map_attribute mapattr[] = {
    //         { "test", "foo", 3, HYPERDATATYPE_INT64, "\x00\x00\x00\x00\x00\x00\x00\x00", 8, HYPERDATATYPE_INT64 }
    //     };
    // }
    // {
    //     struct hyperdex_client_map_attribute mapattr[] = {
    //         { "test", "foo", 3, HYPERDATATYPE_INT64, "\x00\x00\x00\x00\x00\x00\x00\x00", 8, HYPERDATATYPE_INT64 }
    //     };
    // }
// struct hyperdex_client_map_attribute
// {
//     const char* attr; /* NULL-terminated */
//     const char* map_key;
//     size_t map_key_sz;
//     enum hyperdatatype map_key_datatype;
//     const char* value;
//     size_t value_sz;
//     enum hyperdatatype value_datatype;
// };

  // int64_t hyperdex_client_map_atomic_add(struct hyperdex_client* client,
  //         const char* space,
  //         const char* key, size_t key_sz,
  //         const struct hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
  //         enum hyperdex_client_returncode* status);

    #include "../../errors.log"

    printf("%s\n", attr.value);
    printf("op handle: %d, status: %d\n", (int)op_id, op_status);
    printf("loop status: %d\n", loop_status);

    /* perform the "get" */
    op_id = hyperdex_client_get(client, "kv", "some key", 8,
                                &op_status, &attrs, &attrs_sz);

    loop_id = hyperdex_client_loop(client, -1, &loop_status);
    printf("get done\n");
    printf("loop status: %d\n", loop_status);
    printf("op status: %d\n", op_status);

    if (attrs_sz <= 0) {
        printf("Error! Attribute doesn't exist. Line %d", __LINE__);
    } else {
        if (attrs[1].value_sz != attr.value_sz) {
            printf("Error! Attribute has incorrect size. Line %d", __LINE__);
        } else {
            if (memcmp(&attrs[1].value, &attr.value, attr.value_sz) != 0) {
                printf("Error! Attribute values not equal.");
            }
        }
    }


    hyperdex_client_destroy_attrs(attrs, attrs_sz);
    hyperdex_client_destroy(client);
    return EXIT_SUCCESS;
}