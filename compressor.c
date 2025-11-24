// compressor.c
// CPU-only multithreaded compressor using ZSTD + SHA256 (EVP).
// Writes compress/<basename>.cmp (binary) and compress/<basename>.meta (text).

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <zstd.h>
#include <openssl/evp.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>

typedef struct {
    int id;
    unsigned char *data;   // original chunk bytes
    size_t orig_size;
    unsigned char *cdata;  // compressed data
    size_t csize;
    unsigned char sha256[32];
} ChunkJob;

typedef struct {
    ChunkJob *jobs;
    int count;
    int next_idx;
    pthread_mutex_t lock;
} JobQueue;

static void jobqueue_init(JobQueue *q, ChunkJob *jobs, int count) {
    q->jobs = jobs;
    q->count = count;
    q->next_idx = 0;
    pthread_mutex_init(&q->lock, NULL);
}

static ChunkJob* jobqueue_pop(JobQueue *q) {
    pthread_mutex_lock(&q->lock);
    if (q->next_idx >= q->count) { pthread_mutex_unlock(&q->lock); return NULL; }
    ChunkJob *j = &q->jobs[q->next_idx++];
    pthread_mutex_unlock(&q->lock);
    return j;
}

typedef struct {
    JobQueue *queue;
    int zstd_level;
} WorkerArg;

static void compute_sha256_evp(const unsigned char *data, size_t len, unsigned char out[32]) {
    EVP_MD_CTX *mdctx = EVP_MD_CTX_new();
    if (!mdctx) { memset(out, 0, 32); return; }
    EVP_DigestInit_ex(mdctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(mdctx, data, len);
    unsigned int outlen = 0;
    EVP_DigestFinal_ex(mdctx, out, &outlen);
    EVP_MD_CTX_free(mdctx);
}

static void *worker_thread(void *varg) {
    WorkerArg *warg = (WorkerArg*)varg;
    JobQueue *q = warg->queue;
    int level = warg->zstd_level;

    while (1) {
        ChunkJob *job = jobqueue_pop(q);
        if (!job) break;

        // compute SHA256
        compute_sha256_evp(job->data, job->orig_size, job->sha256);

        // compress with ZSTD using per-thread context
        ZSTD_CCtx *cctx = ZSTD_createCCtx();
        if (!cctx) { job->cdata = NULL; job->csize = 0; continue; }

        size_t bound = ZSTD_compressBound(job->orig_size);
        job->cdata = (unsigned char*)malloc(bound);
        if (!job->cdata) { ZSTD_freeCCtx(cctx); job->csize = 0; continue; }

        size_t csz = ZSTD_compressCCtx(cctx, job->cdata, bound, job->data, job->orig_size, level);
        if (ZSTD_isError(csz)) {
            free(job->cdata);
            job->cdata = NULL;
            job->csize = 0;
        } else {
            job->csize = csz;
        }
        ZSTD_freeCCtx(cctx);
    }
    return NULL;
}

static size_t choose_chunk_size(size_t filesize) {
    // Automatic chunk sizing depending on file size
    // small files => small chunks; huge files => bigger chunks
    const size_t KB = 1024;
    const size_t MB = 1024 * KB;

    if (filesize <= 8 * MB) return 1 * MB;      // <=8MB -> 1MB chunks
    if (filesize <= 128 * MB) return 4 * MB;    // <=128MB -> 4MB
    if (filesize <= 1024 * MB) return 16 * MB;  // <=1GB -> 16MB
    return 64 * MB;                              // >1GB -> 64MB
}

static const char* get_basename(const char* path) {
    const char *p = strrchr(path, '/');
    return p ? p+1 : path;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <input.bin> <compress_dir>\n", argv[0]);
        return 1;
    }
    const char *inpath = argv[1];
    const char *outdir = argv[2];

    // create compress directory if needed
    mkdir(outdir, 0755);

    // get filesize
    struct stat st;
    if (stat(inpath, &st) != 0) { perror("stat"); return 1; }
    size_t filesize = (size_t)st.st_size;
    if (filesize == 0) { fprintf(stderr, "Empty file\n"); return 1; }

    size_t chunk_size = choose_chunk_size(filesize);
    int num_chunks = (int)((filesize + chunk_size - 1) / chunk_size);

    printf("File: %s, size=%zu bytes, chunk=%zu, chunks=%d\n", inpath, filesize, chunk_size, num_chunks);

    // allocate jobs
    ChunkJob *jobs = (ChunkJob*)calloc((size_t)num_chunks, sizeof(ChunkJob));
    if (!jobs) { perror("calloc jobs"); return 1; }

    // read the file chunk by chunk into jobs (streaming)
    FILE *fin = fopen(inpath, "rb");
    if (!fin) { perror("open input"); free(jobs); return 1; }

    for (int i = 0; i < num_chunks; ++i) {
        size_t toread = chunk_size;
        if ((size_t)i * chunk_size + toread > filesize) toread = filesize - (size_t)i * chunk_size;
        jobs[i].id = i;
        jobs[i].orig_size = toread;
        jobs[i].data = (unsigned char*)malloc(toread);
        if (!jobs[i].data) { fprintf(stderr, "OOM allocating chunk buffer\n"); fclose(fin); return 1; }
        size_t r = fread(jobs[i].data, 1, toread, fin);
        if (r != toread) { fprintf(stderr, "short read\n"); fclose(fin); return 1; }
        jobs[i].cdata = NULL;
        jobs[i].csize = 0;
    }
    fclose(fin);

    // prepare job queue and worker threads
    JobQueue queue;
    jobqueue_init(&queue, jobs, num_chunks);

    int nthreads = (int)sysconf(_SC_NPROCESSORS_ONLN);
    if (nthreads < 1) nthreads = 1;
    // Cap threads to a reasonable number
    if (nthreads > 16) nthreads = 16;

    pthread_t *threads = (pthread_t*)malloc(nthreads * sizeof(pthread_t));
    WorkerArg warg;
    warg.queue = &queue;
    // choose max zstd level intelligently but safe
    int zstd_max_level = ZSTD_maxCLevel(); // recommended maximum
    // but ZSTD_maxCLevel() can be large; choose 19 or system max whichever smaller
    int zstd_level = (zstd_max_level > 19) ? 19 : zstd_max_level;
    warg.zstd_level = zstd_level;

    printf("Launching %d worker threads; ZSTD level=%d\n", nthreads, zstd_level);

    for (int t = 0; t < nthreads; ++t) {
        pthread_create(&threads[t], NULL, worker_thread, &warg);
    }

    // wait for workers
    for (int t = 0; t < nthreads; ++t) pthread_join(threads[t], NULL);

    // prepare output filenames
    const char *base = get_basename(inpath);
    char out_cmp[1024], out_meta[1024];
    snprintf(out_cmp, sizeof(out_cmp), "%s/%s.cmp", outdir, base);
    snprintf(out_meta, sizeof(out_meta), "%s/%s.meta", outdir, base);

    FILE *fcmp = fopen(out_cmp, "wb");
    if (!fcmp) { perror("open cmp"); return 1; }
    FILE *fmeta = fopen(out_meta, "w");
    if (!fmeta) { perror("open meta"); fclose(fcmp); return 1; }

    // Write binary header to .cmp: original size (8), chunk_size (8), num_chunks (4)
    fwrite(&filesize, sizeof(uint64_t), 1, fcmp);
    uint64_t cs64 = (uint64_t)chunk_size;
    fwrite(&cs64, sizeof(uint64_t), 1, fcmp);
    uint32_t nch32 = (uint32_t)num_chunks;
    fwrite(&nch32, sizeof(uint32_t), 1, fcmp);

    // For each chunk write comp_size (8) then data; write meta line per chunk: id orig_size comp_size sha256hex
    for (int i = 0; i < num_chunks; ++i) {
        // ensure job has compressed data
        size_t csize = jobs[i].csize;
        uint64_t csize64 = (uint64_t)csize;
        fwrite(&csize64, sizeof(uint64_t), 1, fcmp);
        if (csize > 0) fwrite(jobs[i].cdata, 1, csize, fcmp);

        // write meta line: id orig_size comp_size sha256hex
        char hex[65]; hex[64] = 0;
        for (int b = 0; b < 32; ++b) sprintf(hex + b*2, "%02x", jobs[i].sha256[b]);
        fprintf(fmeta, "%d %" PRIu64 " %" PRIu64 " %s\n", jobs[i].id, (uint64_t)jobs[i].orig_size, (uint64_t)csize64, hex);
    }

    fclose(fcmp);
    fclose(fmeta);

    printf("Compression complete: %s and %s\n", out_cmp, out_meta);

    // free memory
    for (int i = 0; i < num_chunks; ++i) {
        if (jobs[i].data) free(jobs[i].data);
        if (jobs[i].cdata) free(jobs[i].cdata);
    }
    free(jobs);
    free(threads);

    return 0;
}
