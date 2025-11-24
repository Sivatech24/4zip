import struct
import tempfile
import os

MAGIC = b'BCMP1'  # file magic to identify format

################################################################################
# BitReader: can accept a path or an already-open file object (seekable)
################################################################################
class BitReader:
    def __init__(self, path_or_file):
        if isinstance(path_or_file, str):
            self.f = open(path_or_file, "rb")
            self._owns_file = True
        else:
            self.f = path_or_file
            self._owns_file = False
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

    def seek(self, pos, whence=0):
        self.f.seek(pos, whence)
        self.buffer = 0
        self.bits_left = 0

    def tell(self):
        return self.f.tell()

    def close(self):
        if self._owns_file:
            self.f.close()



################################################################################
# BitWriter
################################################################################
class BitWriter:
    def __init__(self, path):
        self.f = open(path, "wb")
        self.buffer = 0
        self.bits_filled = 0

    def write_bits(self, value, bitcount):
        # write 'bitcount' bits from 'value' (most-significant first)
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



################################################################################
# Variable-length integer encoding (prefix-based)
# 1-byte:  0 + 7 bits  => 0xxxxxxx (0..127)
# 2-byte: 10 + 14 bits => 10xxxxxx xxxxxxxx (128..16383)
# 3-byte: 110 + 21 bits=> 110xxxxx xxxxxxxx xxxxxxxx (16384..2M+)
################################################################################
def write_varint_to_bitwriter(bw, n):
    if n < 128:
        bw.write_bits(0b0, 1)
        bw.write_bits(n, 7)
    elif n < 16384:
        bw.write_bits(0b10, 2)
        bw.write_bits(n, 14)
    else:
        bw.write_bits(0b110, 3)
        bw.write_bits(n, 21)


def read_varint_from_bitreader(br):
    first = br.read_bit()
    if first is None:
        return None
    if first == 0:
        # read 7 bits
        val = 0
        for _ in range(7):
            b = br.read_bit()
            if b is None: return None
            val = (val << 1) | b
        return val
    second = br.read_bit()
    if second is None:
        return None
    if first == 1 and second == 0:
        # 14 bits
        val = 0
        for _ in range(14):
            b = br.read_bit()
            if b is None: return None
            val = (val << 1) | b
        return val
    # otherwise 3-byte (21 bits)
    val = 0
    for _ in range(21):
        b = br.read_bit()
        if b is None: return None
        val = (val << 1) | b
    return val



################################################################################
# Compressor (builds dictionary while streaming input bits)
# Writes dictionary + payload into single final output file.
################################################################################
def compress(input_path, output_path):
    # temp payload file to store encoded varints as raw bytes (bit-packed)
    tmp_payload = tempfile.NamedTemporaryFile(delete=False)
    tmp_payload_name = tmp_payload.name
    tmp_payload.close()

    br_in = BitReader(input_path)
    bw_tmp = BitWriter(tmp_payload_name)

    # initial dictionary (patterns mapped to indexes)
    dictionary = {"0": 1, "1": 2}
    reverse_dict = {1: "0", 2: "1"}
    next_idx = 3

    current = ""
    total_bits = 0

    try:
        while True:
            bit = br_in.read_bit()
            if bit is None:
                break
            total_bits += 1
            bit = str(bit)
            new = current + bit
            if new in dictionary:
                current = new
            else:
                # output index of current pattern
                write_varint_to_bitwriter(bw_tmp, dictionary[current])
                # add new pattern to dictionary
                dictionary[new] = next_idx
                reverse_dict[next_idx] = new
                next_idx += 1
                current = bit

        if current:
            write_varint_to_bitwriter(bw_tmp, dictionary[current])

    finally:
        br_in.close()
        bw_tmp.finish()

    # Now write final output: MAGIC | num_entries | (idx,len,pattern)* | payload_bytes
    with open(output_path, "wb") as out_f:
        out_f.write(MAGIC)
        # write number of dictionary entries as 4-byte unsigned
        out_f.write(struct.pack(">I", len(reverse_dict)))
        # write each index -> pattern (index 4 bytes, length 2 bytes, pattern bytes)
        # ensure deterministic ordering by index
        for idx in sorted(reverse_dict.keys()):
            pat = reverse_dict[idx].encode("ascii")
            out_f.write(struct.pack(">I", idx))
            out_f.write(struct.pack(">H", len(pat)))
            out_f.write(pat)
        # append payload (already bit-packed)
        with open(tmp_payload_name, "rb") as p:
            while True:
                chunk = p.read(65536)
                if not chunk:
                    break
                out_f.write(chunk)

    os.remove(tmp_payload_name)
    print("Compression complete.")
    print(f"Output: {output_path}")
    print(f"Dictionary entries: {len(reverse_dict)}")
    print(f"Total bits processed: {total_bits}")


################################################################################
# Decompressor (reads embedded dictionary, then payload bits)
################################################################################
def decompress(input_path, output_path):
    with open(input_path, "rb") as f:
        # read and validate header
        magic = f.read(len(MAGIC))
        if magic != MAGIC:
            raise ValueError("Not a BCOMP file (bad magic)")

        num_entries_data = f.read(4)
        if len(num_entries_data) < 4:
            raise ValueError("Truncated file header")
        num_entries = struct.unpack(">I", num_entries_data)[0]

        dictionary = {}
        for _ in range(num_entries):
            hdr = f.read(6)
            if len(hdr) < 6:
                raise ValueError("Truncated dictionary entry")
            idx = struct.unpack(">I", hdr[:4])[0]
            length = struct.unpack(">H", hdr[4:6])[0]
            pat = f.read(length)
            if len(pat) < length:
                raise ValueError("Truncated pattern")
            dictionary[idx] = pat.decode("ascii")

        # current file position is the start of the payload (bit-packed varints)
        payload_start = f.tell()

        # use BitReader on the same open file object but seek to payload_start
        br_payload = BitReader(f)
        br_payload.seek(payload_start)

        bw_out = BitWriter(output_path)

        try:
            while True:
                idx = read_varint_from_bitreader(br_payload)
                if idx is None:
                    break
                if idx not in dictionary:
                    raise ValueError(f"Unknown dictionary index: {idx}")
                pattern = dictionary[idx]
                for ch in pattern:
                    bw_out.write_bits(int(ch), 1)
        finally:
            bw_out.finish()
            # Note: BitReader won't close the file 'f' since we passed file object
            # so we close it here
            f.close()

    print("Decompression complete.")
    print(f"Restored file: {output_path}")


################################################################################
# CLI
################################################################################
def print_usage():
    print("Usage:")
    print("  python bitcompress.py compress <input.bin> <output.bcmp>")
    print("  python bitcompress.py decompress <input.bcmp> <output.bin>")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print_usage()
        sys.exit(1)
    cmd = sys.argv[1].lower()
    if cmd == "compress":
        compress(sys.argv[2], sys.argv[3])
    elif cmd == "decompress":
        decompress(sys.argv[2], sys.argv[3])
    else:
        print_usage()