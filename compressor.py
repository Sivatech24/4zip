import os
from collections import OrderedDict

CHUNK = 1024 * 1024  # 1MB chunk processing

def varint_encode(number: int) -> bytes:
    """Encode integer into variable-length bytes."""
    out = []
    while number > 127:
        out.append((number & 0x7F) | 0x80)
        number >>= 7
    out.append(number & 0x7F)
    return bytes(out)

def compress_file(input_txt, dict_bin, output_bin):
    dictionary = OrderedDict()
    reverse_dict = {}
    next_index = 1

    # open files
    with open(input_txt, "r") as f_in, \
         open(dict_bin, "wb") as f_dict, \
         open(output_bin, "wb") as f_out:

        buffer = ""

        while True:
            chunk = f_in.read(CHUNK)
            if not chunk:
                break
            buffer += chunk

            start = 0
            while start < len(buffer):
                end = start + 1
                while end <= len(buffer):
                    piece = buffer[start:end]

                    if piece in dictionary:
                        end += 1
                        continue
                    else:
                        # NEW PATTERN → add to dictionary
                        dictionary[piece] = next_index
                        reverse_dict[next_index] = piece
                        next_index += 1

                        # Write index (varint) to output
                        parent = dictionary.get(piece[:-1], 0)
                        f_out.write(varint_encode(parent))

                        break
                start = end

        # After scanning, save dictionary in binary form
        for idx, pattern in reverse_dict.items():
            f_dict.write(varint_encode(idx))
            f_dict.write(varint_encode(len(pattern)))
            f_dict.write(pattern.encode("ascii"))


if __name__ == "__main__":
    compress_file("input.txt", "indexfile.bin", "output.bin")
    print("Compression completed!")