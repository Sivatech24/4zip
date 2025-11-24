// compressor.c
// Stream + chunked compression using ZSTD (max level) and per-chunk SHA256 (OpenSSL).
// Optionally calls gpu_sha256(data, len, out32) if provided (returns 0 on success).
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>
#include <pthread.h>
#include <unistd.h>
#include <openssl/sha.h>
#include <zstd.h>
#include <sys/stat.h>
#include <openssl/sha.h>

#ifndef CHUNK_SIZE
// default chunk size: 4MB (you can change via -DCHUNK_SIZE=8388608)
#define CHUNK_SIZE (4 * 1024 * 1024)
#endif

// If you provide a GPU implementation in cuda_sha256.cu, it should expose:
// extern "C" int gpu_sha256(const unsigned char* data, size_t len, unsigned char out32[32]);
// Return 0 on success, non-zero on error (in which case CPU fallback is used).
extern int gpu_sha256(const unsigned char* data, size_t len, unsigned char out32[32]);
/* If the GPU implementation is not linked, the linker will fail.
   To avoid link error, we provide a weak symbol fallback below in the Makefile compilation:
   compile cuda_sha256_stub.c which defines gpu_sha256 returning -1.
*/

typedef struct {
    int id;
    size_t orig_size;
    size_t comp_size;
    unsigned char sha256[32];
    unsigned char *data; // owns the chunk data
} ChunkResult;

typedef struct {
    const char *input_path;
    const char *out_dir;
} JobArgs;

static size_t get_filesize(const char* path) {
    struct stat st;
    if (stat(path, &st) != 0) return 0;
    return (size_t)st.st_size;
}

static void to_hex(const unsigned char *in, size_t len, char *out_hex) {
    static const char hex[] = "0123456789abcdef";
    for (size_t i=0;i<len;i++) {
        out_hex[i*2] = hex[(in[i]>>4)&0xF];
        out_hex[i*2+1] = hex[in[i]&0xF];
    }
    out_hex[len*2] = 0;
}

// Worker compress thread data
typedef struct {
    int thread_id;
    int nthreads;
    const char* inpath;
    FILE* fin; // shared file pointer protected by mutex
    pthread_mutex_t *file_lock;
    size_t chunk_size;
    ChunkResult *results; // preallocated array sized num_chunks
    size_t total_chunks;
} WorkerCtx;

static void compute_sha256_cpu(const unsigned char* data, size_t len, unsigned char out[32]) {
    SHA256_CTX ctx;
    SHA256_Init(&ctx);
    SHA256_Update(&ctx, data, len);
    SHA256_Final(out, &ctx);
}

static int try_gpu_sha256(const unsigned char* data, size_t len, unsigned char out[32]) {
    // Call external GPU function; it should return 0 on success
    // If not linked or returns non-zero, caller will fallback to CPU.
    int res = -1;
    // We call the function pointer if available.
    res = gpu_sha256(data, len, out);
    return res;
}

void* worker_func(void* arg) {
    WorkerCtx* ctx = (WorkerCtx*)arg;
    int tid = ctx->thread_id;
    size_t chunk_size = ctx->chunk_size;
    size_t idx = tid;

    // We'll read chunks from the file with a mutex-protected read pointer
    while (1) {
        size_t read_offset;
        size_t to_read;
        // Determine next chunk index using a global progress via file seek
        pthread_mutex_lock(ctx->file_lock);
        // compute next chunk index from file pointer
        long curr = ftell(ctx->fin);
        if (curr < 0) {
            pthread_mutex_unlock(ctx->file_lock);
            break;
        }
        // get file size
        fseek(ctx->fin, 0, SEEK_END);
        long file_end = ftell(ctx->fin);
        // reset pointer to curr
        fseek(ctx->fin, curr, SEEK_SET);

        if ((size_t)curr >= (size_t)file_end) {
            pthread_mutex_unlock(ctx->file_lock);
            break; // no more chunks
        }
        // determine how many bytes to read
        to_read = (size_t)file_end - (size_t)curr;
        if (to_read > chunk_size) to_read = chunk_size;
        read_offset = (size_t)curr;

        // allocate buffer & read
        unsigned char *buf = (unsigned char*)malloc(to_read);
        if (!buf) { pthread_mutex_unlock(ctx->file_lock); break; }
        size_t r = fread(buf, 1, to_read, ctx->fin);
        pthread_mutex_unlock(ctx->file_lock);
        if (r == 0) { free(buf); break; }

        // compute SHA256 (try GPU first)
        unsigned char sha[32];
        int gres = try_gpu_sha256(buf, r, sha);
        if (gres != 0) {
            compute_sha256_cpu(buf, r, sha);
        }

        // compress chunk with zstd (max level)
        ZSTD_CCtx* cctx = ZSTD_createCCtx();
        if (!cctx) { free(buf); break; }
        int level = ZSTD_maxCLevel(); // maximum compression level supported by lib
        size_t maxc = ZSTD_compressBound(r);
        unsigned char* cbuf = (unsigned char*)malloc(maxc);
        if (!cbuf) { ZSTD_freeCCtx(cctx); free(buf); break; }

        size_t csize = ZSTD_compressCCtx(cctx, cbuf, maxc, buf, r, level);
        if (ZSTD_isError(csize)) {
            // compression failed; free and store as uncompressed block (we'll store comp_size=0 to indicate raw)
            free(cbuf);
            cbuf = NULL;
            csize = 0;
        }

        // determine current chunk id by computing index from read_offset / chunk_size
        size_t chunk_id = read_offset / chunk_size;
        if (chunk_id >= ctx->total_chunks) chunk_id = ctx->total_chunks - 1;

        // fill results slot
        ctx->results[chunk_id].id = (int)chunk_id;
        ctx->results[chunk_id].orig_size = r;
        ctx->results[chunk_id].comp_size = csize;
        memcpy(ctx->results[chunk_id].sha256, sha, 32);

        // store compressed data pointer in results->data as contiguous memory: first 1 byte flag then data
        // flag: 0 = compressed present, 1 = uncompressed raw
        if (csize > 0) {
            ctx->results[chunk_id].data = (unsigned char*)malloc(1 + csize);
            if (!ctx->results[chunk_id].data) { free(buf); if (cbuf) free(cbuf); ZSTD_freeCCtx(cctx); break; }
            ctx->results[chunk_id].data[0] = 0;
            memcpy(ctx->results[chunk_id].data + 1, cbuf, csize);
        } else {
            // store raw
            ctx->results[chunk_id].data = (unsigned char*)malloc(1 + r);
            if (!ctx->results[chunk_id].data) { free(buf); ZSTD_freeCCtx(cctx); break; }
            ctx->results[chunk_id].data[0] = 1;
            memcpy(ctx->results[chunk_id].data + 1, buf, r);
            ctx->results[chunk_id].comp_size = (size_t)r; // store actual bytes stored
        }

        free(buf);
        if (cbuf) free(cbuf);
        ZSTD_freeCCtx(cctx);
    }

    return NULL;
}

int compress_file_streaming(const char* inpath, const char* out_dir) {
    FILE* fin = fopen(inpath, "rb");
    if (!fin) { perror("open input"); return -1; }
    size_t total = get_filesize(inpath);
    if (total == 0) { fclose(fin); fprintf(stderr, "Empty file or stat failure\n"); return -1; }
    size_t chunk_size = CHUNK_SIZE;
    size_t num_chunks = (total + chunk_size - 1) / chunk_size;

    // allocate results
    ChunkResult* results = (ChunkResult*)calloc(num_chunks, sizeof(ChunkResult));
    if (!results) { fclose(fin); return -1; }

    // prepare worker contexts
    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads <= 0) nthreads = 1;
    pthread_t *threads = malloc(sizeof(pthread_t) * nthreads);
    WorkerCtx *wctx = malloc(sizeof(WorkerCtx) * nthreads);
    pthread_mutex_t file_lock; pthread_mutex_init(&file_lock, NULL);

    for (int i=0;i<nthreads;i++) {
        wctx[i].thread_id = i;
        wctx[i].nthreads = nthreads;
        wctx[i].inpath = inpath;
        wctx[i].fin = fin;
        wctx[i].file_lock = &file_lock;
        wctx[i].chunk_size = chunk_size;
        wctx[i].results = results;
        wctx[i].total_chunks = num_chunks;
    }

    // launch threads
    for (int i=0;i<nthreads;i++) pthread_create(&threads[i], NULL, worker_func, &wctx[i]);
    for (int i=0;i<nthreads;i++) pthread_join(threads[i], NULL);

    // build output filenames
    const char* base = strrchr(inpath, '/'); base = base ? base + 1 : inpath;
    char out_cmp[1024], out_meta[1024];
    snprintf(out_cmp, sizeof(out_cmp), "%s/%s.cmp", out_dir, base);
    snprintf(out_meta, sizeof(out_meta), "%s/%s.meta", out_dir, base);

    // write .cmp file (container)
    FILE* fcmp = fopen(out_cmp, "wb");
    if (!fcmp) { perror("create cmp"); fclose(fin); return -1; }
    // header: magic + total_size(8) + chunk_size(8) + num_chunks(8)
    fwrite("ZSTDCP1", 1, 7, fcmp);
    uint64_t total_u64 = (uint64_t) total;
    uint64_t chunk_u64 = (uint64_t) chunk_size;
    uint64_t chunks_u64 = (uint64_t) num_chunks;
    fwrite(&total_u64, sizeof(uint64_t), 1, fcmp);
    fwrite(&chunk_u64, sizeof(uint64_t), 1, fcmp);
    fwrite(&chunks_u64, sizeof(uint64_t), 1, fcmp);

    // Write chunk entries: for each chunk, write 1 byte flag + 8 bytes orig_size + 8 bytes stored_size + data
    for (size_t i=0;i<num_chunks;i++) {
        // ensure we have results
        size_t orig = results[i].orig_size;
        size_t stored = results[i].comp_size;
        unsigned char flag = 0;
        if (results[i].data && results[i].data[0]==1) flag = 1; // raw
        // write: flag (1), orig_size (8), stored_size (8)
        fwrite(&flag, 1, 1, fcmp);
        uint64_t orig64 = (uint64_t)orig;
        uint64_t stored64 = (uint64_t)stored;
        fwrite(&orig64, sizeof(uint64_t), 1, fcmp);
        fwrite(&stored64, sizeof(uint64_t), 1, fcmp);
        if (results[i].data) {
            fwrite(results[i].data + 1, 1, stored, fcmp);
        }
    }
    fclose(fcmp);

    // write meta file (plain text) with: chunk_id orig_size comp_size sha256hex
    FILE* fmeta = fopen(out_meta, "w");
    if (!fmeta) { perror("create meta"); fclose(fin); return -1; }
    for (size_t i=0;i<num_chunks;i++) {
        char hex[65];
        to_hex(results[i].sha256, 32, hex);
        fprintf(fmeta, "%zu %zu %zu %s\n", i, results[i].orig_size, results[i].comp_size, hex);
    }
    fclose(fmeta);

    // cleanup
    for (size_t i=0;i<num_chunks;i++) {
        if (results[i].data) free(results[i].data);
    }
    free(results);
    free(threads);
    free(wctx);
    pthread_mutex_destroy(&file_lock);
    fclose(fin);

    printf("Compressed -> %s\nMetadata -> %s\nChunks: %zu\n", out_cmp, out_meta, num_chunks);
    return 0;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        printf("Usage: %s <input.bin> <compress_dir>\n", argv[0]);
        return 1;
    }
    // ensure out dir exists
    mkdir(argv[2], 0755);
    return compress_file_streaming(argv[1], argv[2]);
}
