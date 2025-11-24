// decompressor.c
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

    size_t total_size;
    size_t chunk_size;
    int num_chunks;
    if (fread(&total_size, sizeof(size_t), 1, fcmp) != 1) { fclose(fcmp); fclose(fmeta); return -1; }
    if (fread(&chunk_size, sizeof(size_t), 1, fcmp) != 1) { fclose(fcmp); fclose(fmeta); return -1; }
    if (fread(&num_chunks, sizeof(int), 1, fcmp) != 1) { fclose(fcmp); fclose(fmeta); return -1; }

    // Prepare output file name
    // meta format: "<id> <orig_size> <comp_size> <hash>\n" per chunk
    char base[512];
    const char* b = strrchr(cmp_path, '/'); b = b ? b+1 : cmp_path;
    snprintf(base, sizeof(base), "%s/%s", out_dir, b);

    mkdir(out_dir, 0755);
    FILE* fout = fopen(base, "wb");
    if (!fout) { perror("create out"); fclose(fcmp); fclose(fmeta); return -1; }

    for (int i = 0; i < num_chunks; ++i) {
        int id; size_t orig_size; int comp_size; unsigned int hash;
        if (fscanf(fmeta, "%d %zu %d %u", &id, &orig_size, &comp_size, &hash) != 4) {
            fprintf(stderr, "meta parse error\n"); break;
        }
        int cs = comp_size;
        if (cs <= 0) {
            // handle uncompressed or error (skip)
            // In our flow, cs should be > 0
            continue;
        }
        // read compressed size
        int read_cs;
        if (fread(&read_cs, sizeof(int), 1, fcmp) != 1) { fprintf(stderr, "cmp read size failed\n"); break; }
        if (read_cs != cs) { fprintf(stderr, "mismatch cs\n"); }
        char* comp_buf = malloc(cs);
        if (!comp_buf) break;
        if (fread(comp_buf, 1, cs, fcmp) != (size_t)cs) { free(comp_buf); break; }

        char* out_buf = malloc(orig_size);
        if (!out_buf) { free(comp_buf); break; }
        int dec = LZ4_decompress_safe(comp_buf, out_buf, cs, (int)orig_size);
        if (dec < 0) {
            fprintf(stderr, "LZ4 decompress failed for chunk %d\n", i);
        } else {
            fwrite(out_buf, 1, dec, fout);
        }
        free(comp_buf);
        free(out_buf);
    }

    fclose(fout);
    fclose(fcmp);
    fclose(fmeta);
    printf("Decompressed to %s\n", base);
    return 0;
}

int main(int argc, char** argv) {
    if (argc != 4) {
        printf("Usage: %s <cmp_file> <meta_file> <decompress_dir>\n", argv[0]);
        return 1;
    }
    return decompress_file(argv[1], argv[2], argv[3]);
}
