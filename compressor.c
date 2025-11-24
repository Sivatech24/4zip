// compressor.c
// Multi-threaded compressor using LZ4 + GPU hashing helper.
// Build with nvcc or gcc linking the cuda object.

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>
#include <lz4.h>
#include <dirent.h>

extern int gpu_hash_chunks(unsigned char* h_buf, size_t total_bytes, size_t chunk_size, int num_chunks, uint32_t* out_hashes);

typedef struct {
    int id;
    unsigned char* data;
    size_t size;
    char* out_buf;
    int out_size;
} CompressJob;

typedef struct {
    CompressJob* jobs;
    int count;
    int next;
    pthread_mutex_t lock;
} JobQueue;

JobQueue g_queue;

void jobqueue_init(JobQueue* q, CompressJob* jobs, int count) {
    q->jobs = jobs; q->count = count; q->next = 0;
    pthread_mutex_init(&q->lock, NULL);
}

CompressJob* jobqueue_pop(JobQueue* q) {
    pthread_mutex_lock(&q->lock);
    if (q->next >= q->count) { pthread_mutex_unlock(&q->lock); return NULL; }
    CompressJob* j = &q->jobs[q->next++];
    pthread_mutex_unlock(&q->lock);
    return j;
}

void* worker_thread(void* arg) {
    (void)arg;
    while (1) {
        CompressJob* job = jobqueue_pop(&g_queue);
        if (!job) break;
        // allocate output buffer: worst-case LZ4 bound
        int maxDstSize = LZ4_compressBound((int)job->size);
        job->out_buf = malloc(maxDstSize);
        if (!job->out_buf) { job->out_size = -1; continue; }
        int compressed = LZ4_compress_default((const char*)job->data, job->out_buf, (int)job->size, maxDstSize);
        if (compressed <= 0) {
            job->out_size = -1;
        } else {
            job->out_size = compressed;
        }
    }
    return NULL;
}

int compress_file(const char* input_path, const char* out_dir) {
    FILE* f = fopen(input_path, "rb");
    if (!f) { perror("open input"); return -1; }
    fseek(f, 0, SEEK_END);
    size_t total = ftell(f);
    fseek(f, 0, SEEK_SET);

    const size_t CHUNK = 4 * 1024 * 1024; // 4 MB chunk
    int num_chunks = (int)((total + CHUNK - 1) / CHUNK);

    unsigned char* whole_buf = (unsigned char*)malloc(total);
    if (!whole_buf) { fclose(f); return -1; }
    size_t read = fread(whole_buf, 1, total, f);
    fclose(f);
    if (read != total) { free(whole_buf); return -1; }

    // GPU compute chunk hashes (fast)
    uint32_t* hashes = (uint32_t*)malloc(num_chunks * sizeof(uint32_t));
    if (!hashes) { free(whole_buf); return -1; }
    int gres = gpu_hash_chunks(whole_buf, total, CHUNK, num_chunks, hashes);
    if (gres != 0) {
        fprintf(stderr, "GPU hashing failed (code %d) — continuing without hashes\n", gres);
        for (int i=0;i<num_chunks;i++) hashes[i] = 0;
    }

    // Create jobs
    CompressJob* jobs = (CompressJob*)calloc(num_chunks, sizeof(CompressJob));
    for (int i = 0; i < num_chunks; ++i) {
        size_t start = (size_t)i * CHUNK;
        size_t remain = (start + CHUNK <= total) ? CHUNK : (total - start);
        jobs[i].id = i;
        jobs[i].data = whole_buf + start;
        jobs[i].size = remain;
        jobs[i].out_buf = NULL;
        jobs[i].out_size = 0;
    }

    jobqueue_init(&g_queue, jobs, num_chunks);

    // Launch worker threads
    int nthreads = sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;
    pthread_t* threads = malloc(nthreads * sizeof(pthread_t));
    for (int t = 0; t < nthreads; ++t) pthread_create(&threads[t], NULL, worker_thread, NULL);

    for (int t = 0; t < nthreads; ++t) pthread_join(threads[t], NULL);

    // Write output files: compressed file and meta
    char out_cmp[1024], out_meta[1024];
    const char* base = strrchr(input_path, '/');
    base = base ? base + 1 : input_path;
    snprintf(out_cmp, sizeof(out_cmp), "%s/%s.cmp", out_dir, base);
    snprintf(out_meta, sizeof(out_meta), "%s/%s.meta", out_dir, base);

    FILE* fcmp = fopen(out_cmp, "wb");
    FILE* fmeta = fopen(out_meta, "wb");
    if (!fcmp || !fmeta) { perror("create out"); return -1; }

    // Write a small header: original size (8 bytes), chunk size (4), num_chunks (4)
    fwrite(&total, sizeof(size_t), 1, fcmp);
    fwrite(&CHUNK, sizeof(size_t), 1, fcmp);
    fwrite(&num_chunks, sizeof(int), 1, fcmp);

    // Now write each chunk compressed size and data
    for (int i = 0; i < num_chunks; ++i) {
        // store metadata line: id, orig_size, comp_size, hash
        fprintf(fmeta, "%d %zu %d %u\n", i, jobs[i].size, jobs[i].out_size, hashes[i]);

        // write comp size (4 bytes) then data
        int cs = jobs[i].out_size;
        fwrite(&cs, sizeof(int), 1, fcmp);
        if (cs > 0) fwrite(jobs[i].out_buf, 1, cs, fcmp);
        // free output buffer
        if (jobs[i].out_buf) free(jobs[i].out_buf);
    }

    fclose(fcmp);
    fclose(fmeta);
    free(jobs);
    free(hashes);
    free(whole_buf);
    free(threads);

    printf("Compressed -> %s and metadata -> %s\n", out_cmp, out_meta);
    return 0;
}

int main(int argc, char** argv) {
    if (argc != 3) {
        printf("Usage: %s <input.bin> <compress_dir>\n", argv[0]);
        return 1;
    }
    const char* in = argv[1];
    const char* outdir = argv[2];

    // ensure compress dir exists
    mkdir(outdir, 0755);
    return compress_file(in, outdir);
}
