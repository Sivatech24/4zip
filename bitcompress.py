import struct

###############################################################
# BIT READER — Streams bits from huge files without loading RAM
###############################################################

class BitReader:
    def __init__(self, path):
        self.f = open(path, "rb")
        self.buffer = 0
        self.bits_left = 0

    def read_bit(self):
        if self.bits_left == 0:
            byte = self.f.read(1)
            if not byte:
                return None  # EOF
            self.buffer = byte[0]
            self.bits_left = 8

        self.bits_left -= 1
        return (self.buffer >> self.bits_left) & 1

    def close(self):
        self.f.close()



###############################################################
# BIT WRITER — Write variable-length bit sequences to output
###############################################################

class BitWriter:
    def __init__(self, path):
        self.f = open(path, "wb")
        self.buffer = 0
        self.bits_filled = 0

    def write_bits(self, value, bitcount):
        for i in reversed(range(bitcount)):
            bit = (value >> i) & 1
            self.buffer = (self.buffer << 1) | bit
            self.bits_filled += 1

            if self.bits_filled == 8:
                self.f.write(bytes([self.buffer]))
                self.buffer = 0
                self.bits_filled = 0

    def finish(self):
        if self.bits_filled > 0:
            self.buffer <<= (8 - self.bits_filled)
            self.f.write(bytes([self.buffer]))
        self.f.close()



###############################################################
# VARIABLE-LENGTH ENCODING FOR INDEXES
#
# 1–127: 1 byte   (0xxxxxxx)
# 128–16383: 2 bytes (10xxxxxx xxxxxxxx)
# 16384–2M+: 3 bytes (110xxxxx xxxxxxxx xxxxxxxx)
#
###############################################################

def write_varint(bw, n):
    if n < 128:
        bw.write_bits(0b0, 1)
        bw.write_bits(n, 7)
    elif n < 16384:
        bw.write_bits(0b10, 2)
        bw.write_bits(n, 14)
    else:
        bw.write_bits(0b110, 3)
        bw.write_bits(n, 21)


def read_varint(bits):
    first = bits.read_bit()
    if first == 0:
        # 1-byte value (7 bits)
        val = 0
        for _ in range(7):
            b = bits.read_bit()
            if b is None: return None
            val = (val << 1) | b
        return val

    second = bits.read_bit()
    if first == 1 and second == 0:
        # 2-byte value (14 bits)
        val = 0
        for _ in range(14):
            b = bits.read_bit()
            if b is None: return None
            val = (val << 1) | b
        return val
    
    # 3-byte (21 bits)
    val = 0
    for _ in range(21):
        b = bits.read_bit()
        if b is None: return None
        val = (val << 1) | b
    return val



###############################################################
# COMPRESSOR — BIT-LEVEL PATTERN DETECTION + STREAMING
###############################################################

def compress(input_file, output_file, dict_file="dictionary.bin"):
    br = BitReader(input_file)
    bw = BitWriter(output_file)

    dictionary = {"0": 1, "1": 2}
    reverse_dict = {1:"0", 2:"1"}
    next_idx = 3

    current = ""
    total_bits = 0

    while True:
        bit = br.read_bit()
        if bit is None:
            break
        total_bits += 1

        bit = str(bit)
        new = current + bit

        if new in dictionary:
            current = new
        else:
            write_varint(bw, dictionary[current])
            dictionary[new] = next_idx
            reverse_dict[next_idx] = new
            next_idx += 1
            current = bit

    if current:
        write_varint(bw, dictionary[current])

    br.close()
    bw.finish()

    # Save binary dictionary
    with open(dict_file, "wb") as df:
        for idx, pat in reverse_dict.items():
            df.write(struct.pack(">I", idx))
            df.write(struct.pack(">H", len(pat)))
            df.write(pat.encode("ascii"))

    print("Compression complete!")
    print(f"Patterns: {next_idx-1}")
    print(f"Total bits processed: {total_bits}")



###############################################################
# DECOMPRESSOR — REBUILD BITSTREAM FROM INDEX SEQUENCE
###############################################################

def load_dictionary(path):
    d = {}
    with open(path, "rb") as f:
        while True:
            block = f.read(6)
            if not block:
                break
            idx = struct.unpack(">I", block[:4])[0]
            length = struct.unpack(">H", block[4:6])[0]
            pattern = f.read(length).decode("ascii")
            d[idx] = pattern
    return d


def decompress(input_file, output_file, dict_file="dictionary.bin"):
    dictionary = load_dictionary(dict_file)

    br = BitReader(input_file)
    out = BitWriter(output_file)

    while True:
        idx = read_varint(br)
        if idx is None:
            break
        pattern = dictionary[idx]

        # Write bit by bit
        for b in pattern:
            out.write_bits(int(b), 1)

    br.close()
    out.finish()
    print("Decompression complete!")



###############################################################
# CLI USAGE
###############################################################

if __name__ == "__main__":
    import sys

    if len(sys.argv) < 4:
        print("Usage:")
        print("  python bitcompress.py compress input.bin output.cmp")
        print("  python bitcompress.py decompress input.cmp output.bin")
        sys.exit(0)

    mode = sys.argv[1]

    if mode == "compress":
        compress(sys.argv[2], sys.argv[3])
    elif mode == "decompress":
        decompress(sys.argv[2], sys.argv[3])
    else:
        print("Unknown mode.")