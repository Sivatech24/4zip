// cuda_hash.cu
#include <cuda_runtime.h>
#include <stdint.h>
#include <cstdio>
#include <cstdlib>

extern "C" {

__device__ inline uint32_t fnv1a32_device(const unsigned char* data, size_t len) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (uint32_t)data[i];
        hash *= 16777619u;
    }
    return hash;
}

__global__ void kernel_hash_chunk(const unsigned char* d_buf, size_t len, uint32_t* out_hash) {
    // single-threaded device hash (len may be up to chunk size)
    if (threadIdx.x == 0 && blockIdx.x == 0) {
        *out_hash = fnv1a32_device(d_buf, len);
    }
}

int gpu_hash_chunk(const unsigned char* h_buf, size_t len, uint32_t* out_hash_host) {
    if (!h_buf || !out_hash_host) return -1;
    unsigned char* d_buf = nullptr;
    uint32_t* d_hash = nullptr;
    cudaError_t err;

    err = cudaMalloc((void**)&d_buf, len);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMalloc d_buf failed: %s\n", cudaGetErrorString(err));
        return -1;
    }
    err = cudaMemcpy(d_buf, h_buf, len, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMemcpy H->D failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf);
        return -2;
    }
    err = cudaMalloc((void**)&d_hash, sizeof(uint32_t));
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMalloc d_hash failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf);
        return -3;
    }

    kernel_hash_chunk<<<1, 1>>>(d_buf, len, d_hash);
    err = cudaGetLastError();
    if (err != cudaSuccess) {
        fprintf(stderr, "kernel launch failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf); cudaFree(d_hash);
        return -4;
    }

    err = cudaMemcpy(out_hash_host, d_hash, sizeof(uint32_t), cudaMemcpyDeviceToHost);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMemcpy D->H failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf); cudaFree(d_hash);
        return -5;
    }

    cudaFree(d_buf);
    cudaFree(d_hash);
    return 0;
}

} // extern "C"
