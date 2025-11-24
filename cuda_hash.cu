// cuda_hash.cu
// Simple CUDA kernel to compute a 32-bit fnv1a-like hash per block.
// Exported function: gpu_hash_chunks(unsigned char* data, size_t chunk_size, int num_chunks, unsigned int* out_hashes)

#include <cuda_runtime.h>
#include <stdint.h>
#include <cstdio>

extern "C" {

__device__ inline uint32_t fnv1a32(const unsigned char* data, size_t len) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (uint32_t)data[i];
        hash *= 16777619u;
    }
    return hash;
}

__global__ void kernel_hash_blocks(const unsigned char* d_buf, size_t chunk_size, int num_chunks, uint32_t* out_hashes, size_t total_bytes) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= num_chunks) return;

    size_t start = (size_t)idx * chunk_size;
    size_t remain = (start + chunk_size <= total_bytes) ? chunk_size : (total_bytes - start);
    const unsigned char* ptr = d_buf + start;
    out_hashes[idx] = fnv1a32(ptr, remain);
}

int gpu_hash_chunks(unsigned char* h_buf, size_t total_bytes, size_t chunk_size, int num_chunks, uint32_t* out_hashes_host) {
    unsigned char* d_buf = nullptr;
    uint32_t* d_hashes = nullptr;
    size_t buf_bytes = total_bytes;

    cudaError_t err = cudaMalloc((void**)&d_buf, buf_bytes);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMalloc d_buf failed: %s\n", cudaGetErrorString(err));
        return -1;
    }
    err = cudaMemcpy(d_buf, h_buf, buf_bytes, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMemcpy H->D failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf);
        return -2;
    }
    err = cudaMalloc((void**)&d_hashes, num_chunks * sizeof(uint32_t));
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMalloc d_hashes failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf);
        return -3;
    }

    int threads = 256;
    int blocks = (num_chunks + threads - 1) / threads;
    kernel_hash_blocks<<<blocks, threads>>>(d_buf, chunk_size, num_chunks, d_hashes, buf_bytes);
    err = cudaGetLastError();
    if (err != cudaSuccess) {
        fprintf(stderr, "kernel launch failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf); cudaFree(d_hashes);
        return -4;
    }

    err = cudaMemcpy(out_hashes_host, d_hashes, num_chunks * sizeof(uint32_t), cudaMemcpyDeviceToHost);
    if (err != cudaSuccess) {
        fprintf(stderr, "cudaMemcpy D->H failed: %s\n", cudaGetErrorString(err));
        cudaFree(d_buf); cudaFree(d_hashes);
        return -5;
    }

    cudaFree(d_buf);
    cudaFree(d_hashes);
    return 0;
}

} // extern "C"
