// compressor.c
// CPU-only, multithreaded chunked compressor using ZSTD and SHA-256.
// Output:
//  compress/<basename>.cmp  -> binary container: [total_size:uint64][chunk_size:uint32][num_chunks:uint32][chunk0_comp_size:uint32][chunk0_data]...
//  compress/<basename>.meta -> text lines: chunk_id orig_size comp_size sha256hex

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <zstd.h>
#include <openssl/sha.h>
#include <sys/stat.h>
#include <unistd.h>

#ifndef CHUNK_SIZE
#define CHUNK_SIZE (4 * 1024 * 1024) // 4MB default chunk
#endif

typedef struct {
    int id;
    size_t orig_size;
    unsigned char *data;     // owns data
    size_t comp_size;
    void *comp_buf;          // owns comp_buf
    unsigned char sha256[32];
} Job;

typedef struct {
    Job **jobs;
    int count;
    int next;
    pthread_mutex_t lock;
} JobQueue;

static JobQueue gq;

void jobqueue_init(JobQueue *q, Job **jobs, int count) {
    q->jobs = jobs;
    q->count = count;
    q->next = 0;
    pthread_mutex_init(&q->lock, NULL);
}

Job* jobqueue_pop(JobQueue *q) {
    pthread_mutex_lock(&q->lock);
    if (q->next >= q->count) { pthread_mutex_unlock(&q->lock); return NULL; }
    Job *j = q->jobs[q->next++];
    pthread_mutex_unlock(&q->lock);
    return j;
}

void *worker_thread(void *arg) {
    (void)arg;
    Job *job;
    while ((job = jobqueue_pop(&gq)) != NULL) {
        // compute SHA-256
        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, job->data, job->orig_size);
        SHA256_Final(job->sha256, &ctx);

        // allocate comp buffer (bound)
        size_t bound = ZSTD_compressBound((size_t)job->orig_size);
        job->comp_buf = malloc(bound);
        if (!job->comp_buf) { job->comp_size = 0; continue; }

        // compress with level = max (you asked "C = Max compression")
        int level = ZSTD_maxCLevel();
        size_t csize = ZSTD_compress(job->comp_buf, bound, job->data, job->orig_size, level);
        if (ZSTD_isError(csize)) {
            // fallback: try default level
            csize = ZSTD_compress(job->comp_buf, bound, job->data, job->orig_size, 3);
            if (ZSTD_isError(csize)) {
                free(job->comp_buf);
                job->comp_buf = NULL;
                job->comp_size = 0;
                continue;
            }
        }
        job->comp_size = csize;
        // free original chunk data to reduce RAM (we only need comp_buf and meta)
        free(job->data);
        job->data = NULL;
    }
    return NULL;
}

static int ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) return 0;
    return mkdir(path, 0755);
}

static void hex_sha256(unsigned char *sha, char outhex[65]) {
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < 32; ++i) {
        outhex[i*2] = hex[(sha[i] >> 4) & 0xF];
        outhex[i*2+1] = hex[sha[i] & 0xF];
    }
    outhex[64] = 0;
}

int compress_file(const char *input_path, const char *out_dir) {
    FILE *fin = fopen(input_path, "rb");
    if (!fin) { perror("open input"); return -1; }

    // determine file size
    struct stat st;
    if (stat(input_path, &st) != 0) { perror("stat"); fclose(fin); return -1; }
    uint64_t total_size = (uint64_t)st.st_size;

    // compute number of chunks
    uint32_t chunk_size = CHUNK_SIZE;
    uint32_t num_chunks = (uint32_t)((total_size + chunk_size - 1) / chunk_size);

    if (ensure_dir(out_dir) != 0) {
        // ignore if exists
    }

    // derive base name
    const char *base = strrchr(input_path, '/');
    base = base ? base+1 : input_path;

    char out_cmp[1024], out_meta[1024];
    snprintf(out_cmp, sizeof(out_cmp), "%s/%s.cmp", out_dir, base);
    snprintf(out_meta, sizeof(out_meta), "%s/%s.meta", out_dir, base);

    FILE *fcmp = fopen(out_cmp, "wb");
    if (!fcmp) { perror("create cmp"); fclose(fin); return -1; }
    FILE *fmeta = fopen(out_meta, "w");
    if (!fmeta) { perror("create meta"); fclose(fin); fclose(fcmp); return -1; }

    // write header to cmp: total_size (8), chunk_size (4), num_chunks (4)
    fwrite(&total_size, sizeof(uint64_t), 1, fcmp);
    fwrite(&chunk_size, sizeof(uint32_t), 1, fcmp);
    fwrite(&num_chunks, sizeof(uint32_t), 1, fcmp);

    // Prepare jobs - stream read chunks
    Job **jobs = (Job**)calloc(num_chunks, sizeof(Job*));
    if (!jobs) { fclose(fin); fclose(fcmp); fclose(fmeta); return -1; }

    for (uint32_t i = 0; i < num_chunks; ++i) {
        size_t this_sz = (size_t)((i == num_chunks-1) ? (total_size - (uint64_t)i*chunk_size) : chunk_size);
        unsigned char *buf = malloc(this_sz);
        if (!buf) { perror("malloc chunk"); return -1; }
        size_t r = fread(buf, 1, this_sz, fin);
        if (r != this_sz) { fprintf(stderr, "read short\n"); free(buf); return -1; }

        Job *j = (Job*)calloc(1, sizeof(Job));
        j->id = i;
        j->orig_size = this_sz;
        j->data = buf;
        j->comp_buf = NULL;
        j->comp_size = 0;
        jobs[i] = j;
    }
    fclose(fin);

    // init job queue and launch workers
    jobqueue_init(&gq, jobs, num_chunks);
    int nthreads = sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;
    pthread_t *threads = malloc(nthreads * sizeof(pthread_t));
    for (int t = 0; t < nthreads; ++t) pthread_create(&threads[t], NULL, worker_thread, NULL);

    for (int t = 0; t < nthreads; ++t) pthread_join(threads[t], NULL);

    // Write compressed chunks to cmp and metadata to meta
    for (uint32_t i = 0; i < num_chunks; ++i) {
        Job *j = jobs[i];
        // write comp_size (4 bytes) then comp data
        uint32_t cs = (uint32_t)j->comp_size;
        fwrite(&cs, sizeof(uint32_t), 1, fcmp);
        if (cs > 0 && j->comp_buf) {
            fwrite(j->comp_buf, 1, cs, fcmp);
        }
        char shahex[65];
        hex_sha256(j->sha256, shahex);
        fprintf(fmeta, "%u %zu %u %s\n", (unsigned)i, j->orig_size, cs, shahex);

        if (j->comp_buf) free(j->comp_buf);
        free(j);
    }

    free(jobs);
    free(threads);
    fclose(fcmp);
    fclose(fmeta);
    printf("Compressed -> %s and metadata -> %s\n", out_cmp, out_meta);
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <input.bin> <compress_dir>\n", argv[0]);
        return 1;
    }
    return compress_file(argv[1], argv[2]);
}
