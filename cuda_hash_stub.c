// cuda_sha256_stub.c
#include <stddef.h>
#include <stdint.h>
int gpu_sha256(const unsigned char* data, size_t len, unsigned char out32[32]) {
    // stub: indicate GPU not available; return non-zero to force CPU fallback
    (void)data; (void)len; (void)out32;
    return -1;
}
