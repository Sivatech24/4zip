import os
from collections import OrderedDict

CHUNK = 1024 * 1024  # process 1MB at a time


def bytes_to_bits(data: bytes) -> str:
    """Convert raw bytes to bitstring."""
    return ''.join(f'{byte:08b}' for byte in data)


def varint_encode(num: int) -> bytes:
    """Variable-length integer encoding."""
    out = []
    while num > 127:
        out.append((num & 0x7F) | 0x80)
        num >>= 7
    out.append(num)
    return bytes(out)


def compress_file(input_bin, dict_bin, output_bin):
    dictionary = OrderedDict()      # pattern → index
    reverse = {}                    # index → pattern
    next_index = 1

    with open(input_bin, "rb") as f_in, \
         open(dict_bin, "wb") as f_dict, \
         open(output_bin, "wb") as f_out:

        buffer = ""

        while True:
            chunk = f_in.read(CHUNK)
            if not chunk:
                break

            # Convert bytes → bits
            bitstream = bytes_to_bits(chunk)
            buffer += bitstream

            i = 0
            while i < len(buffer):
                j = i + 1
                while j <= len(buffer):
                    piece = buffer[i:j]

                    if piece in dictionary:
                        j += 1
                        continue
                    else:
                        # new pattern
                        dictionary[piece] = next_index
                        reverse[next_index] = piece
                        next_index += 1

                        # Write parent pattern index
                        parent = dictionary.get(piece[:-1], 0)
                        f_out.write(varint_encode(parent))
                        break
                i = j

        # Save dictionary in binary form
        for idx, pat in reverse.items():
            f_dict.write(varint_encode(idx))
            f_dict.write(varint_encode(len(pat)))
            f_dict.write(pat.encode("ascii"))


if __name__ == "__main__":
    compress_file("test1/input.bin", "test1/indexfile.bin", "test1/output.bin")
    print("Compression done.")