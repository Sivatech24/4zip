CC = gcc
CFLAGS = -O3 -march=native -Wall
LDFLAGS = -lzstd -lcrypto -lpthread

all: compressor decompressor

compressor: compressor.c
	$(CC) $(CFLAGS) compressor.c -o compressor $(LDFLAGS)

decompressor: decompressor.c
	$(CC) $(CFLAGS) decompressor.c -o decompressor -lzstd

clean:
	rm -f compressor decompressor
