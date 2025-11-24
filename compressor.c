// compressor.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <lz4.h>
#include <errno.h>

#ifdef __cplusplus
extern "C" {
#endif
int gpu_hash_chunk(const unsigned char* h_buf, size_t len, uint32_t* out_hash_host);
#ifdef __cplusplus
}
#endif

// Default chunk size: 64 MB (change to 128 * 1024 * 1024 for 128 MB)
#ifndef CHUNK_SIZE
#define CHUNK_SIZE (64ULL * 1024ULL * 1024ULL)
#endif

typedef struct {
    int id;
    unsigned char* data; // owns this buffer
    size_t size;
    char* comp_buf;      // compressed data (allocated)
    int comp_size;       // compressed size
    uint32_t hash;
} Job;

typedef struct {
    Job* jobs;
    int count;
    int next;
    pthread_mutex_t lock;
} JobQueue;

static JobQueue g_queue;

void jobqueue_init(JobQueue* q, Job* jobs, int count) {
    q->jobs = jobs;
    q->count = count;
    q->next = 0;
    pthread_mutex_init(&q->lock, NULL);
}

Job* jobqueue_pop(JobQueue* q) {
    pthread_mutex_lock(&q->lock);
    if (q->next >= q->count) { pthread_mutex_unlock(&q->lock); return NULL; }
    Job* j = &q->jobs[q->next++];
    pthread_mutex_unlock(&q->lock);
    return j;
}

void* worker_thread(void* arg) {
    (void)arg;
    while (1) {
        Job* job = jobqueue_pop(&g_queue);
        if (!job) break;
        int maxDst = LZ4_compressBound((int)job->size);
        job->comp_buf = (char*)malloc(maxDst);
        if (!job->comp_buf) { job->comp_size = -1; continue; }
        int cs = LZ4_compress_default((const char*)job->data, job->comp_buf, (int)job->size, maxDst);
        if (cs <= 0) {
            job->comp_size = -1;
        } else {
            job->comp_size = cs;
        }
    }
    return NULL;
}

static uint32_t cpu_hash_fnv1a(const unsigned char* data, size_t len) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < len; ++i) {
        hash ^= (uint32_t)data[i];
        hash *= 16777619u;
    }
    return hash;
}

int compress_file_streaming(const char* input_path, const char* out_dir) {
    FILE* fin = fopen(input_path, "rb");
    if (!fin) {
        perror("open input");
        return -1;
    }

    // get file size
    if (fseek(fin, 0, SEEK_END) != 0) { perror("fseek"); fclose(fin); return -1; }
    uint64_t total_size = (uint64_t)ftell(fin);
    fseek(fin, 0, SEEK_SET);

    size_t chunk = CHUNK_SIZE;
    int num_chunks = (int)((total_size + chunk - 1) / chunk);

    if (num_chunks <= 0) {
        fclose(fin);
        fprintf(stderr, "Nothing to compress\n");
        return -1;
    }

    // allocate job array (num_chunks small for typical files)
    Job* jobs = (Job*)calloc((size_t)num_chunks, sizeof(Job));
    if (!jobs) { fclose(fin); fprintf(stderr, "calloc jobs failed\n"); return -1; }

    // read chunks and initialize jobs
    for (int i = 0; i < num_chunks; ++i) {
        size_t to_read = (size_t)chunk;
        uint64_t remain = total_size - ((uint64_t)i * chunk);
        if (remain < to_read) to_read = (size_t)remain;

        unsigned char* buf = (unsigned char*)malloc(to_read);
        if (!buf) { fprintf(stderr, "malloc chunk failed\n"); return -1; }
        size_t r = fread(buf, 1, to_read, fin);
        if (r != to_read) {
            fprintf(stderr, "read error: expected %zu got %zu\n", to_read, r);
            free(buf);
            return -1;
        }
        jobs[i].id = i;
        jobs[i].data = buf;
        jobs[i].size = to_read;
        jobs[i].comp_buf = NULL;
        jobs[i].comp_size = 0;

        // compute hash: try GPU first, else CPU fallback
        uint32_t h = 0;
        int gres = gpu_hash_chunk(buf, to_read, &h);
        if (gres != 0) {
            // GPU not available or failed -> CPU hash
            h = cpu_hash_fnv1a(buf, to_read);
        }
        jobs[i].hash = h;
    }

    fclose(fin);

    // prepare queue and workers
    jobqueue_init(&g_queue, jobs, num_chunks);
    int nthreads = sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;
    pthread_t* threads = (pthread_t*)malloc(nthreads * sizeof(pthread_t));
    for (int t = 0; t < nthreads; ++t) pthread_create(&threads[t], NULL, worker_thread, NULL);

    for (int t = 0; t < nthreads; ++t) pthread_join(threads[t], NULL);

    // Build output paths
    const char* baseptr = strrchr(input_path, '/');
    const char* basename = baseptr ? baseptr + 1 : input_path;
    char out_cmp[1024]; char out_meta[1024];
    snprintf(out_cmp, sizeof(out_cmp), "%s/%s.cmp", out_dir, basename);
    snprintf(out_meta, sizeof(out_meta), "%s/%s.meta", out_dir, basename);

    // ensure directory exists
    mkdir(out_dir, 0755);

    // Write compressed container
    FILE* fcmp = fopen(out_cmp, "wb");
    FILE* fmeta = fopen(out_meta, "w");
    if (!fcmp || !fmeta) { perror("create out files"); return -1; }

    // header: total_size (8 bytes), chunk_size (8), num_chunks (4)
    fwrite(&total_size, sizeof(uint64_t), 1, fcmp);
    uint64_t chunk_u64 = (uint64_t)chunk;
    fwrite(&chunk_u64, sizeof(uint64_t), 1, fcmp);
    fwrite(&num_chunks, sizeof(int), 1, fcmp);

    // write chunks in order
    for (int i = 0; i < num_chunks; ++i) {
        int cs = jobs[i].comp_size;
        // meta line: id orig_size comp_size hash
        fprintf(fmeta, "%d %zu %d %u\n", i, jobs[i].size, cs, jobs[i].hash);

        // write comp_size (4 bytes) then data if available
        fwrite(&cs, sizeof(int), 1, fcmp);
        if (cs > 0) {
            fwrite(jobs[i].comp_buf, 1, cs, fcmp);
        } else {
            // compression failed, write raw uncompressed data: write -1 then raw bytes length and bytes
            int raw_flag = -1;
            fwrite(&raw_flag, sizeof(int), 1, fcmp); // marker
            uint64_t orig_s = jobs[i].size;
            fwrite(&orig_s, sizeof(uint64_t), 1, fcmp);
            fwrite(jobs[i].data, 1, jobs[i].size, fcmp);
        }
        // free chunk buffers
        if (jobs[i].comp_buf) free(jobs[i].comp_buf);
        if (jobs[i].data) free(jobs[i].data);
    }

    fclose(fcmp);
    fclose(fmeta);
    free(jobs);
    free(threads);

    printf("Compressed -> %s and metadata -> %s\n", out_cmp, out_meta);
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        printf("Usage: %s <input.bin> <compress_dir>\n", argv[0]);
        return 1;
    }
    return compress_file_streaming(argv[1], argv[2]);
}
