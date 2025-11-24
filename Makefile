CC = gcc
CFLAGS = -O3 -Wall -march=native -pthread
LDLIBS = -lzstd -lcrypto

all: compressor decompressor

compressor: compressor.c
	$(CC) $(CFLAGS) compressor.c -o compressor $(LDLIBS)

decompressor: decompressor.c
	$(CC) $(CFLAGS) decompressor.c -o decompressor $(LDLIBS)

clean:
	rm -f compressor decompressor
