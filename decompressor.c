// decompressor.c
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <lz4.h>
#include <sys/stat.h>
#include <unistd.h>

int decompress_file(const char* cmp_path, const char* meta_path, const char* out_dir) {
    FILE* fcmp = fopen(cmp_path, "rb");
    if (!fcmp) { perror("open cmp"); return -1; }
    FILE* fmeta = fopen(meta_path, "r");
    if (!fmeta) { perror("open meta"); fclose(fcmp); return -1; }

    uint64_t total_size;
    uint64_t chunk_size;
    int num_chunks;
    if (fread(&total_size, sizeof(uint64_t), 1, fcmp) != 1) { fprintf(stderr, "bad header\n"); fclose(fcmp); fclose(fmeta); return -1; }
    if (fread(&chunk_size, sizeof(uint64_t), 1, fcmp) != 1) { fprintf(stderr, "bad header\n"); fclose(fcmp); fclose(fmeta); return -1; }
    if (fread(&num_chunks, sizeof(int), 1, fcmp) != 1) { fprintf(stderr, "bad header\n"); fclose(fcmp); fclose(fmeta); return -1; }

    // create output path
    const char* baseptr = strrchr(cmp_path, '/');
    const char* basename = baseptr ? baseptr + 1 : cmp_path;
    char out_path[1024];
    snprintf(out_path, sizeof(out_path), "%s/%s", out_dir, basename);

    mkdir(out_dir, 0755);
    FILE* fout = fopen(out_path, "wb");
    if (!fout) { perror("create out"); fclose(fcmp); fclose(fmeta); return -1; }

    // Read meta lines and then read compressed blocks in same order
    for (int i = 0; i < num_chunks; ++i) {
        int id; size_t orig_size; int comp_size; unsigned int hash;
        if (fscanf(fmeta, "%d %zu %d %u", &id, &orig_size, &comp_size, &hash) != 4) {
            fprintf(stderr, "meta parse error at chunk %d\n", i); break;
        }

        // read comp_size field from cmp file
        int cs_from_file;
        if (fread(&cs_from_file, sizeof(int), 1, fcmp) != 1) { fprintf(stderr, "cmp read size failed\n"); break; }

        if (cs_from_file == -1) {
            // raw uncompressed block was stored
            uint64_t orig_s;
            if (fread(&orig_s, sizeof(uint64_t), 1, fcmp) != 1) { fprintf(stderr, "raw size read failed\n"); break; }
            unsigned char* buf = malloc((size_t)orig_s);
            if (!buf) break;
            if (fread(buf, 1, (size_t)orig_s, fcmp) != orig_s) { free(buf); break; }
            fwrite(buf, 1, (size_t)orig_s, fout);
            free(buf);
        } else {
            if (cs_from_file <= 0) {
                fprintf(stderr, "invalid comp size %d\n", cs_from_file);
                break;
            }
            char* comp_buf = malloc(cs_from_file);
            if (!comp_buf) break;
            if (fread(comp_buf, 1, cs_from_file, fcmp) != (size_t)cs_from_file) { free(comp_buf); break; }

            char* out_buf = malloc(orig_size);
            if (!out_buf) { free(comp_buf); break; }
            int dec = LZ4_decompress_safe(comp_buf, out_buf, cs_from_file, (int)orig_size);
            if (dec < 0) {
                fprintf(stderr, "LZ4 decompress failed for chunk %d\n", i);
                // fallback: write nothing or attempt something else
            } else {
                fwrite(out_buf, 1, dec, fout);
            }
            free(comp_buf);
            free(out_buf);
        }
    }

    fclose(fout);
    fclose(fcmp);
    fclose(fmeta);
    printf("Decompressed to %s\n", out_path);
    return 0;
}

int main(int argc, char** argv) {
    if (argc != 4) {
        printf("Usage: %s <cmp_file> <meta_file> <decompress_dir>\n", argv[0]);
        return 1;
    }
    return decompress_file(argv[1], argv[2], argv[3]);
}
