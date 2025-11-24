NVCC = nvcc
CC = gcc
CFLAGS = -O3 -Wall -march=native
LDFLAGS = -llz4 -lpthread

all: compressor decompressor

cuda_hash.o: cuda_hash.cu
	$(NVCC) -O3 -Xcompiler -fPIC -c cuda_hash.cu -o cuda_hash.o

compressor: compressor.c cuda_hash.o
	$(CC) $(CFLAGS) compressor.c cuda_hash.o -o compressor $(LDFLAGS) -L/usr/local/cuda/lib64 -lcudart

decompressor: decompressor.c
	$(CC) $(CFLAGS) decompressor.c -o decompressor $(LDFLAGS)

clean:
	rm -f compressor decompressor cuda_hash.o
