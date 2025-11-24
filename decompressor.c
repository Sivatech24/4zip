// decompressor.c
// Reads <compress_dir>/<name>.cmp and .meta and reconstructs original file into <decompress_dir>/<name>

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <zstd.h>
#include <openssl/sha.h>
#include <sys/stat.h>
#include <unistd.h>

static int ensure_dir(const char *path) {
    struct stat st;
    if (stat(path, &st) == 0) return 0;
    return mkdir(path, 0755);
}

static void hex_to_bin(const char *hex, unsigned char *out) {
    for (int i = 0; i < 32; ++i) {
        char a = hex[i*2], b = hex[i*2+1];
        unsigned char va = (a>='a') ? (10 + a - 'a') : (a>='A' ? 10 + a - 'A' : a - '0');
        unsigned char vb = (b>='a') ? (10 + b - 'a') : (b>='A' ? 10 + b - 'A' : b - '0');
        out[i] = (va << 4) | (vb & 0xF);
    }
}

int decompress_file(const char *cmp_path, const char *meta_path, const char *out_dir) {
    FILE *fcmp = fopen(cmp_path, "rb");
    if (!fcmp) { perror("open cmp"); return -1; }
    FILE *fmeta = fopen(meta_path, "r");
    if (!fmeta) { perror("open meta"); fclose(fcmp); return -1; }

    uint64_t total_size;
    uint32_t chunk_size;
    uint32_t num_chunks;
    if (fread(&total_size, sizeof(uint64_t), 1, fcmp) != 1) { fprintf(stderr,"bad header\n"); fclose(fcmp); fclose(fmeta); return -1; }
    if (fread(&chunk_size, sizeof(uint32_t), 1, fcmp) != 1) { fprintf(stderr,"bad header\n"); fclose(fcmp); fclose(fmeta); return -1; }
    if (fread(&num_chunks, sizeof(uint32_t), 1, fcmp) != 1) { fprintf(stderr,"bad header\n"); fclose(fcmp); fclose(fmeta); return -1; }

    if (ensure_dir(out_dir) != 0) { /* ignore */ }

    // determine base name for output
    const char *base = strrchr(cmp_path, '/');
    base = base ? base+1 : cmp_path;
    char out_path[1024];
    snprintf(out_path, sizeof(out_path), "%s/%s", out_dir, base);

    // if cmp filename has .cmp, strip or keep? We'll write same base name (with .cmp) as earlier compressor did.
    FILE *fout = fopen(out_path, "wb");
    if (!fout) { perror("create out"); fclose(fcmp); fclose(fmeta); return -1; }

    // We'll read meta lines to know orig_size and expected sha, and read comp_size + data in same order
    for (uint32_t i = 0; i < num_chunks; ++i) {
        unsigned int id;
        size_t orig_size;
        unsigned int comp_size;
        char shahex[128];
        if (fscanf(fmeta, "%u %zu %u %127s", &id, &orig_size, &comp_size, shahex) != 4) {
            fprintf(stderr, "meta parse error at chunk %u\n", i); break;
        }

        // read comp_size from cmp file (should match)
        uint32_t cs_from_file;
        if (fread(&cs_from_file, sizeof(uint32_t), 1, fcmp) != 1) { fprintf(stderr,"cmp truncated\n"); break; }
        if (cs_from_file != comp_size) {
            fprintf(stderr, "comp size mismatch chunk %u (meta %u file %u)\n", i, comp_size, cs_from_file);
            // continue but try to trust cs_from_file
        }
        uint32_t cs_read = cs_from_file;

        if (cs_read == 0) {
            // nothing compressed (shouldn't happen here)
            continue;
        }

        void *cbuf = malloc(cs_read);
        if (!cbuf) break;
        if (fread(cbuf, 1, cs_read, fcmp) != cs_read) { free(cbuf); break; }

        void *outbuf = malloc(orig_size);
        if (!outbuf) { free(cbuf); break; }

        int dec = ZSTD_decompress(outbuf, orig_size, cbuf, cs_read);
        if (ZSTD_isError(dec)) {
            fprintf(stderr, "ZSTD decompress error chunk %u: %s\n", i, ZSTD_getErrorName(dec));
            free(cbuf); free(outbuf); break;
        }
        // validate sha256
        unsigned char sha_expected[32];
        hex_to_bin(shahex, sha_expected);
        unsigned char sha_now[32];
        SHA256(outbuf, dec, sha_now);
        if (memcmp(sha_now, sha_expected, 32) != 0) {
            fprintf(stderr, "SHA mismatch on chunk %u\n", i);
            // still write, but warn
        }
        fwrite(outbuf, 1, dec, fout);
        free(cbuf); free(outbuf);
    }

    fclose(fout);
    fclose(fcmp);
    fclose(fmeta);
    printf("Decompressed to %s\n", out_path);
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <cmp_file> <meta_file> <decompress_dir>\n", argv[0]);
        return 1;
    }
    return decompress_file(argv[1], argv[2], argv[3]);
}
