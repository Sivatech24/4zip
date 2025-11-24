NVCC = nvcc
CC   = gcc
CFLAGS = -O3 -Wall -march=native
LDFLAGS = -lzstd -lssl -lcrypto -lpthread -L/usr/local/cuda/lib64 -lcudart

all: compressor decompressor

cuda_sha256.o: cuda_sha256.cu
	$(NVCC) -O3 -Xcompiler -fPIC -c cuda_sha256.cu -o cuda_sha256.o

cuda_sha256.o: cuda_sha256.cu
	$(NVCC) -O3 -Xcompiler -fPIC -c cuda_sha256.cu -o cuda_sha256.o

compressor: compressor.c cuda_sha256.o
	$(CC) $(CFLAGS) compressor.c cuda_sha256.o -o compressor $(LDFLAGS) -L/usr/local/cuda/lib64 -lcudart

clean:
	rm -f compressor decompressor *.o
