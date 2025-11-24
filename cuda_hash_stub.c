// cuda_hash_stub.c
#include <stdint.h>
int gpu_hash_chunk(const unsigned char* h_buf, size_t len, uint32_t* out_hash_host) {
    // signal GPU not available
    (void)h_buf; (void)len;
    if (out_hash_host) *out_hash_host = 0;
    return -1;
}
