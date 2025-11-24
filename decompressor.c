// decompressor.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <zstd.h>
#include <sys/stat.h>
#include <string.h>

static size_t read_u64(FILE* f, uint64_t* out) {
    if (fread(out, sizeof(uint64_t), 1, f) != 1) return 0;
    return 1;
}

int decompress_container(const char* cmp_path, const char* meta_path, const char* out_dir) {
    FILE* fcmp = fopen(cmp_path, "rb");
    if (!fcmp) { perror("open cmp"); return -1; }
    // read header
    char magic[8]; memset(magic,0,sizeof(magic));
    if (fread(magic, 1, 7, fcmp) != 7) { fclose(fcmp); return -1; }
    if (strncmp(magic, "ZSTDCP1", 7) != 0) { fprintf(stderr, "Bad magic\n"); fclose(fcmp); return -1; }
    uint64_t total_size, chunk_size, num_chunks;
    if (!read_u64(fcmp, &total_size)) { fclose(fcmp); return -1; }
    if (!read_u64(fcmp, &chunk_size)) { fclose(fcmp); return -1; }
    if (!read_u64(fcmp, &num_chunks)) { fclose(fcmp); return -1; }

    // ensure out dir exists
    mkdir(out_dir, 0755);

    // output path
    const char* base = strrchr(cmp_path, '/'); base = base ? base + 1 : cmp_path;
    char outpath[1024];
    snprintf(outpath, sizeof(outpath), "%s/%s", out_dir, base);
    // remove .cmp suffix if present
    char *dot = strrchr(outpath, '.');
    if (dot && strcmp(dot, ".cmp") == 0) *dot = 0;

    FILE* fout = fopen(outpath, "wb");
    if (!fout) { perror("create out"); fclose(fcmp); return -1; }

    for (uint64_t i=0;i<num_chunks;i++) {
        unsigned char flag;
        if (fread(&flag, 1, 1, fcmp) != 1) { fprintf(stderr, "read flag fail\n"); break; }
        uint64_t orig_size, stored_size;
        if (!read_u64(fcmp, &orig_size)) { fprintf(stderr, "read orig_size fail\n"); break; }
        if (!read_u64(fcmp, &stored_size)) { fprintf(stderr, "read stored_size fail\n"); break; }

        if (flag == 1) {
            // raw copy
            unsigned char *buf = malloc(stored_size);
            if (!buf) { fprintf(stderr,"OOM\n"); break; }
            if (fread(buf, 1, stored_size, fcmp) != stored_size) { free(buf); break; }
            fwrite(buf, 1, stored_size, fout);
            free(buf);
        } else {
            // compressed block
            unsigned char *cbuf = malloc(stored_size);
            if (!cbuf) { fprintf(stderr,"OOM\n"); break; }
            if (fread(cbuf, 1, stored_size, fcmp) != stored_size) { free(cbuf); break; }
            unsigned char *outbuf = malloc(orig_size);
            if (!outbuf) { free(cbuf); break; }
            size_t dec = ZSTD_decompress(outbuf, orig_size, cbuf, stored_size);
            if (ZSTD_isError(dec)) {
                fprintf(stderr, "Decompress error chunk %zu: %s\n", (size_t)i, ZSTD_getErrorName(dec));
                free(outbuf); free(cbuf); break;
            }
            fwrite(outbuf, 1, dec, fout);
            free(outbuf);
            free(cbuf);
        }
    }

    fclose(fout);
    fclose(fcmp);
    printf("Decompressed to %s\n", outpath);
    return 0;
}

int main(int argc, char** argv) {
    if (argc != 4) {
        printf("Usage: %s <cmp_file> <meta_file> <decompress_dir>\n", argv[0]);
        return 1;
    }
    return decompress_container(argv[1], argv[2], argv[3]);
}
