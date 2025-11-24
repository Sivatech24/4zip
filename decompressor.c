// decompressor.c
// Decompress files created by compressor.c

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <zstd.h>
#include <sys/stat.h>
#include <unistd.h>
#include <inttypes.h>

static const char* basename_from_path(const char* path) {
    const char *p = strrchr(path, '/');
    return p ? p+1 : path;
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <cmp_file> <meta_file> <decompress_dir>\n", argv[0]);
        return 1;
    }
    const char *cmp_path = argv[1];
    const char *meta_path = argv[2];
    const char *out_dir = argv[3];

    mkdir(out_dir, 0755);

    FILE *fcmp = fopen(cmp_path, "rb");
    if (!fcmp) { perror("open cmp"); return 1; }

    // read header
    uint64_t orig_size;
    uint64_t chunk_size;
    uint32_t num_chunks;
    if (fread(&orig_size, sizeof(uint64_t), 1, fcmp) != 1) { fprintf(stderr, "bad file header\n"); fclose(fcmp); return 1; }
    if (fread(&chunk_size, sizeof(uint64_t), 1, fcmp) != 1) { fprintf(stderr, "bad file header\n"); fclose(fcmp); return 1; }
    if (fread(&num_chunks, sizeof(uint32_t), 1, fcmp) != 1) { fprintf(stderr, "bad file header\n"); fclose(fcmp); return 1; }

    // Open meta to read per-chunk orig_size and comp_size order
    FILE *fmeta = fopen(meta_path, "r");
    if (!fmeta) { perror("open meta"); fclose(fcmp); return 1; }

    // prepare output file path
    const char *base = basename_from_path(cmp_path);
    char outpath[1024];
    snprintf(outpath, sizeof(outpath), "%s/%s", out_dir, base);
    // remove .cmp suffix if present
    size_t blen = strlen(outpath);
    if (blen > 4 && strcmp(outpath + blen - 4, ".cmp") == 0) outpath[blen - 4] = '\0';

    FILE *fout = fopen(outpath, "wb");
    if (!fout) { perror("create out"); fclose(fcmp); fclose(fmeta); return 1; }

    // For each chunk, read comp_size (8) then compressed data from cmp file, and read meta entry to know orig_size
    for (uint32_t i = 0; i < num_chunks; ++i) {
        uint64_t csize64;
        if (fread(&csize64, sizeof(uint64_t), 1, fcmp) != 1) { fprintf(stderr, "cmp corrupted\n"); break; }
        size_t csize = (size_t)csize64;

        // read meta line: id orig_size comp_size sha256hex
        int mid;
        uint64_t orig_sz_meta;
        uint64_t comp_sz_meta;
        char shahex[65];
        if (fscanf(fmeta, "%d %" SCNu64 " %" SCNu64 " %64s\n", &mid, &orig_sz_meta, &comp_sz_meta, shahex) != 4) {
            fprintf(stderr, "meta parse error\n"); break;
        }
        // read compressed bytes
        unsigned char *cbuf = NULL;
        if (csize > 0) {
            cbuf = (unsigned char*)malloc(csize);
            if (!cbuf) { fprintf(stderr, "OOM\n"); break; }
            if (fread(cbuf, 1, csize, fcmp) != csize) { fprintf(stderr, "cmp read short\n"); free(cbuf); break; }
        }

        // allocate output buf for decompressed chunk
        size_t out_size_expected = (size_t)orig_sz_meta;
        unsigned char *outbuf = (unsigned char*)malloc(out_size_expected);
        if (!outbuf) { fprintf(stderr, "OOM outbuf\n"); if (cbuf) free(cbuf); break; }

        if (csize == 0) {
            // nothing compressed? write zeros or skip; here skip
            free(outbuf);
            if (cbuf) free(cbuf);
            continue;
        }

        size_t r = ZSTD_decompress(outbuf, out_size_expected, cbuf, csize);
        if (ZSTD_isError(r)) {
            fprintf(stderr, "Decompress error chunk %d: %s\n", i, ZSTD_getErrorName(r));
            free(outbuf); free(cbuf); break;
        }
        // write decompressed bytes
        fwrite(outbuf, 1, r, fout);

        free(outbuf);
        if (cbuf) free(cbuf);
    }

    fclose(fout);
    fclose(fcmp);
    fclose(fmeta);

    printf("Decompressed to %s\n", outpath);
    return 0;
}
